from aiohttp import web
from aiohttp_apispec import docs, request_schema, response_schema, setup_aiohttp_apispec
import json
import logging
import logging.config
from marshmallow import Schema, fields, exceptions
import os
import sys
import traceback
from typing import Dict, Optional

from bearer_auth import BearerAuth
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


class DownloadRequestSchema(SearchRequestSchema):
    callback_uri = fields.Url(required=True)


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

        self._setup_logging()

        self.ffmpeg = FFmpeg(
            os.path.realpath(server_config.get('spool', '.')),
            os.path.realpath(server_config.get('storage', '.')),
            username=self._config.get('hikvision').get('username'),
            password=self._config.get('hikvision').get('password'),
            **self._config.get('ffmpeg_pool', {})
        )

        self._port = server_config.get('port', DEFAULT_PORT)

        api_key = server_config.get('api_key', None)
        self._api_key = BearerAuth(api_key) if api_key is not None else None

        self._protected_routes = {}

    def _setup_logging(self):
        log_config = self._config.get('server', {}).get('log', {})

        self._logger = logging.getLogger('frankamera')

        if 'path' in log_config:
            os.makedirs(log_config['path'], exist_ok=True)
            handler = logging.FileHandler(os.path.join(log_config['path'], 'frankamera.log'))

            access_logger = logging.getLogger('aiohttp.access')
            access_logger.setLevel(logging.INFO)
            access_logger.addHandler(logging.FileHandler(os.path.join(log_config['path'], 'access.log')))
        else:
            handler = logging.StreamHandler(sys.stderr)

        handler.setFormatter(
            logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S%z')
        )

        self._logger.setLevel(log_config.get('level', DEFAULT_LOG_LEVEL).upper())
        self._logger.addHandler(handler)

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
    @request_schema(DownloadRequestSchema())
    @response_schema(JobSchema())
    async def download(self, request: web.Request):
        try:
            data = DownloadRequestSchema().load(await request.json())

            camera = self.dvr.get_camera_by_id(data['camera_id'])
            result = self.dvr.search(camera, data['start_time'], data['end_time'])

            filename = '{}_{}_{}.mp4'.format(
                camera.name.replace(' ', '-'),
                result.start_time.strftime('%Y%m%dT%H%M%S%z'),
                result.end_time.strftime('%Y%m%dT%H%M%S%z')
            )

            job = self.ffmpeg.download(
                result.rtsp_uri,
                result.start_time,
                result.end_time,
                filename,
                data['callback_uri']
            )

            return web.json_response(JobSchema().dump(job))
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

    @staticmethod
    def _frame_summary_to_tuple(frame: traceback.FrameSummary) -> Dict:
        return {
            'filename': frame.filename,
            'lineno': frame.lineno,
            'name': frame.name,
            'line': frame.line,
            'locals': frame.locals
        }

    @web.middleware
    async def process_request(self, request: web.Request, handler) -> web.Response:
        error = None
        error_status = 500

        try:
            name = request.match_info.route.name
            if self._api_key is not None and name is not None and name in self._protected_routes:
                authorized_header = request.headers.getone('Authorization', None)
                if not authorized_header:
                    raise web.HTTPUnauthorized(reason='No API key')

                api_key = BearerAuth.decode(authorized_header)
                if api_key != self._api_key:
                    self._logger.warning('Invalid API key: {} != {}'.format(api_key, self._api_key))
                    raise web.HTTPUnauthorized(reason='Invalid API key')

            response = await handler(request)
        except Exception as ex:
            self._logger.error(''.join(traceback.format_exception(type(ex), ex, ex.__traceback__, chain=True)))

            response = None

            if isinstance(ex, json.JSONDecodeError):
                error = {'error': str(ex)}
                error_status = 400
            elif isinstance(ex, exceptions.ValidationError):
                error = {'error': 'Validation error', 'extra': ex.messages}
                error_status = 400
            elif isinstance(ex, KeyError):
                error = {'error': 'Key {} not found'.format(str(ex))}
                error_status = 400
            elif isinstance(ex, web.HTTPClientError) or isinstance(ex, web.HTTPError):
                error = {'error': ex.reason}
                error_status = ex.status
            elif isinstance(ex, web.HTTPException):
                response = ex
            else:
                error = {'error': 'Unknown error: {}'.format(str(ex)), 'extra': {'class': str(ex.__class__)}}
                error_status = 500

            if self._config.get('debug', False):
                error['extra'] = error.get('extra', {})
                error['extra']['exception'] = str(ex.__class__)
                error['extra']['message'] = str(ex)
                error['extra']['backtrace'] = [
                    self._frame_summary_to_tuple(frame) for frame in traceback.extract_tb(ex.__traceback__)
                ]

            if not response:
                response = web.json_response(ErrorResponseSchema().dump(error), status=error_status)

        response.headers['server'] = 'Frankamera'

        return response

    def add_route(self, method: str, route: str, handler, name: Optional[str] = None, **kwargs):
        if name is not None:
            self._protected_routes[name] = True
        return web.route(method, route, handler, name=name, **kwargs)

    def run(self):
        app = web.Application(middlewares=[self.process_request])
        app.add_routes([
            self.add_route('GET', '/cameras', self.cameras, name='cameras', allow_head=False),
            self.add_route('POST', '/search', self.search, name='search'),
            self.add_route('POST', '/download', self.download, name='download'),
            self.add_route('GET', '/job/{job_id}', self.job, name='job', allow_head=False),
            self.add_route('GET', '/active_jobs', self.active_jobs, name='active_jobs', allow_head=False),
        ])

        setup_aiohttp_apispec(
            app,
            title='Frankamera',
            version='1',
            url='/api/docs/swagger.json',
            swagger_path='/',
            securityDefinitions={
                'api_key': {
                    'type': 'apiKey',
                    'in': 'header',
                    'name': 'Authorization',
                },
            },
            security=[{'api_key': []}]
        )

        web.run_app(app, port=self._port)


if __name__ == '__main__':
    Frankamera().run()
