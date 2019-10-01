from aiohttp import web
from aiohttp_apispec import docs, request_schema, response_schema, setup_aiohttp_apispec
import json
from marshmallow import Schema, fields, exceptions

from ffmpeg import FFmpeg, JobSchema
from hikvision import (
    Hikvision,
    CameraSchema,
    ResultSchema,
    CameraNotFoundException,
    InvalidRangeException,
    RangeNotFoundException
)


class ErrorResponseSchema(Schema):
    error = fields.String(default=None)
    extra = fields.Dict()


class SearchRequestSchema(Schema):
    camera_id = fields.Integer(required=True)
    start_time = fields.DateTime(required=True)
    end_time = fields.DateTime(required=True)


class Frankamera(object):
    def __init__(self, config='frankamera.json'):
        with open(config) as fd:
            self.config = json.load(fd)

        self.dvr = Hikvision(
            self.config['hikvision']['base_url'],
            self.config['hikvision']['username'],
            self.config['hikvision']['password'],
            self.config['cameras']
        )

        self.ffmpeg = FFmpeg(**self.config.get('ffmpeg_pool', {}))

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

            job_id = self.ffmpeg.download(result.rtsp_uri, result.end_time - result.start_time)
            return web.json_response({'job_id': job_id})
        except CameraNotFoundException as ex:
            raise web.HTTPNotFound(reason=str(ex))
        except InvalidRangeException as ex:
            raise web.HTTPConflict(reason=str(ex))
        except RangeNotFoundException as ex:
            raise web.HTTPRequestRangeNotSatisfiable(reason=str(ex))

    @docs(
        summary='Get the progress of a download job',
        responses={
            500: {'schema': ErrorResponseSchema}
        }
    )
    @response_schema(JobSchema(many=True))
    async def jobs(self, request: web.Request):
        return web.json_response(JobSchema(many=True).dump(self.ffmpeg.get_jobs()))

    @web.middleware
    async def process_response(self, request: web.Request, handler) -> web.Response:
        try:
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
        except web.HTTPError as ex:
            response = web.json_response(ErrorResponseSchema().dump({'error': ex.reason}), status=ex.status)
        except web.HTTPException as ex:
            response = ex
        except Exception as ex:
            response = web.json_response(ErrorResponseSchema().dump({'error': str(ex)}), status=500)

        response.headers['server'] = 'Frankamera'

        return response


if __name__ == '__main__':
    frankamera = Frankamera()

    app = web.Application(middlewares=[frankamera.process_response])
    app.add_routes([
        web.get('/cameras', frankamera.cameras),
        web.post('/search', frankamera.search),
        web.post('/download', frankamera.download),
        web.get('/jobs', frankamera.jobs)
    ])

    setup_aiohttp_apispec(app, title='Frankamera', version='1', url='/api/docs/swagger.json', swagger_path='/')

    web.run_app(app)
