from aiohttp import web
from aiohttp_apispec import docs, request_schema, response_schema, setup_aiohttp_apispec
import json
import logging
from marshmallow import Schema, fields, exceptions
import os
import sys

from ffmpeg import FFmpeg, JobSchema
from hikvision import (
    Hikvision,
    CameraSchema,
    ResultSchema,
    CameraNotFoundException,
    InvalidRangeException,
    RangeNotFoundException
)

DEFAULT_PORT = 19340
DEFAULT_LOG_LEVEL = 'info'


class ErrorResponseSchema(Schema):
    error = fields.String(default=None)
    extra = fields.Dict()
    backtrace = fields.Dict()


class SearchRequestSchema(Schema):
    camera_id = fields.Integer(required=True)
    start_time = fields.DateTime(required=True)
    end_time = fields.DateTime(required=True)


class Frankamera(object):
    def __init__(self, config='frankamera.json'):
        with open(config) as fd:
            self._config = json.load(fd)

        self.dvr = Hikvision(
            self._config.get('hikvision').get('base_url'),
            self._config.get('hikvision').get('username'),
            self._config.get('hikvision').get('password'),
            self._config.get('cameras', {})
        )

        server_config = self._config.get('server', {})

        log_config = server_config.get('log', {})

        logger = logging.getLogger('frankamera')
        logger.setLevel(log_config.get('level', DEFAULT_LOG_LEVEL).upper())

        if 'path' in log_config:
            os.makedirs(log_config['path'], exist_ok=True)
            logger.addHandler(logging.FileHandler(os.path.join(log_config['path'], 'frankamera.log')))
        else:
            logger.addHandler(logging.StreamHandler(sys.stderr))

        self.ffmpeg = FFmpeg(
            os.path.realpath(server_config.get('spool', '.')),
            os.path.realpath(server_config.get('storage', '.')),
            username=self._config.get('hikvision').get('username'),
            password=self._config.get('hikvision').get('password'),
            **self._config.get('ffmpeg_pool', {})
        )

        self._port = server_config.get('port', DEFAULT_PORT)
        self._api_key = server_config.get('api_key', None)

    @docs(
        summary='Get the registered cameras',
        responses={
            500: {'schema': ErrorResponseSchema}
        }
    )
    @response_schema(CameraSchema(many=True))
    async def cameras(self, request: web.Request):
        return web.json_response(CameraSchema(many=True).dump([camera for camera in self.dvr.cameras.values()]))

    @docs(
        summary='Search for stored video data',
        responses={
            400: {'schema': ErrorResponseSchema},
            404: {'schema': ErrorResponseSchema, 'description': 'Camera not found'},
            409: {'schema': ErrorResponseSchema, 'description': 'The given range is invalid'},
            416: {'schema': ErrorResponseSchema, 'description': 'No video data for the given range and camera'},
            500: {'schema': ErrorResponseSchema}
        }
    )
    @request_schema(SearchRequestSchema())
    @response_schema(ResultSchema())
    async def search(self, request: web.Request):
        try:
            data = SearchRequestSchema().load(await request.json())
            result = self.dvr.search(
                self.dvr.get_camera_by_id(data['camera_id']),
                data['start_time'],
                data['end_time']
            )
            return web.json_response(ResultSchema().dump(result))
        except CameraNotFoundException as ex:
            raise web.HTTPNotFound(reason=str(ex))
        except InvalidRangeException as ex:
            raise web.HTTPConflict(reason=str(ex))
        except RangeNotFoundException as ex:
            raise web.HTTPRequestRangeNotSatisfiable(reason=str(ex))

    @docs(
        summary='Download video data',
        responses={
            200: {'description': 'Video data'},
            400: {'schema': ErrorResponseSchema},
            404: {'schema': ErrorResponseSchema, 'description': 'Camera not found'},
            409: {'schema': ErrorResponseSchema, 'description': 'The given range is invalid'},
            416: {'schema': ErrorResponseSchema, 'description': 'No video data for the given range and camera'},
            500: {'schema': ErrorResponseSchema}
        }
    )
    @request_schema(ResultSchema())
    async def download(self, request: web.Request):
        try:
            data = ResultSchema().load(await request.json())
            camera = self.dvr.get_camera_by_id(data['camera_id'])

            result = self.dvr.search(camera, data['start_time'], data['end_time'])

            filename = '{}_{}_{}.mp4'.format(
                camera.name.replace(' ', '-'),
                result.start_time.strftime('%Y%m%dT%H%M%S%z'),
                result.end_time.strftime('%Y%m%dT%H%M%S%z')
            )

            job_id = self.ffmpeg.download(result.rtsp_uri, result.start_time, result.end_time, filename)

            return web.json_response({'job_id': job_id})
        except CameraNotFoundException as ex:
            raise web.HTTPNotFound(reason=str(ex))
        except InvalidRangeException as ex:
            raise web.HTTPConflict(reason=str(ex))
        except RangeNotFoundException as ex:
            raise web.HTTPRequestRangeNotSatisfiable(reason=str(ex))

    @docs(
        summary='Get information about a job',
        responses={
            404: {'schema': ErrorResponseSchema, 'description': 'Job not found'},
            500: {'schema': ErrorResponseSchema}
        }
    )
    @response_schema(JobSchema())
    async def job(self, request: web.Request):
        job = self.ffmpeg.get_job_by_id(request.match_info['job_id'])
        if job is None:
            raise web.HTTPNotFound(reason='Job not found')

        return web.json_response(JobSchema().dump(job))

    @docs(
        summary='Get information about all the jobs',
        responses={
            500: {'schema': ErrorResponseSchema}
        }
    )
    @response_schema(JobSchema(many=True))
    async def active_jobs(self, request: web.Request):
        return web.json_response(JobSchema(many=True).dump(self.ffmpeg.get_all_active_jobs()))

    @web.middleware
    async def process_request(self, request: web.Request, handler) -> web.Response:
        try:
            if request.headers.getone('Authorization', None) != self._api_key:
                raise web.HTTPUnauthorized(reason='Invalid API key')

            response = await handler(request)
        except json.JSONDecodeError as ex:
            response = web.json_response(
                ErrorResponseSchema().dump({'error': str(ex)}),
                status=400
            )
        except exceptions.ValidationError as ex:
            response = web.json_response(
                ErrorResponseSchema().dump({'error': 'Validation error', 'extra': ex.messages}),
                status=400
            )
        except web.HTTPClientError as ex:
            response = web.json_response(ErrorResponseSchema().dump({'error': ex.reason}), status=ex.status)
        except web.HTTPError as ex:
            response = web.json_response(ErrorResponseSchema().dump({'error': ex.reason}), status=ex.status)
        except web.HTTPException as ex:
            response = ex
        except Exception as ex:
            import traceback
            traceback.print_tb(ex.__traceback__)

            response = web.json_response(
                ErrorResponseSchema().dump({
                    'error': 'Unknown error: {}'.format(str(ex)),
                    'extra': {'class': str(ex.__class__)},
                }),
                status=500
            )

        response.headers['server'] = 'Frankamera'

        return response

    def run(self):
        app = web.Application(middlewares=[self.process_request])
        app.add_routes([
            web.get('/cameras', self.cameras, allow_head=False),
            web.post('/search', self.search),
            web.post('/download', self.download),
            web.get('/job/{job_id}', self.job, allow_head=False),
            web.get('/active_jobs', self.active_jobs, allow_head=False),
        ])

        setup_aiohttp_apispec(app, title='Frankamera', version='1', url='/api/docs/swagger.json', swagger_path='/')

        web.run_app(app, port=self._port)


if __name__ == '__main__':
    Frankamera().run()
