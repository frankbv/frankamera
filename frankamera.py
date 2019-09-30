from aiohttp import web
from aiohttp_apispec import docs, request_schema, response_schema, setup_aiohttp_apispec
import json
from marshmallow import Schema, fields

import ffmpeg
from hikvision import (
    Hikvision,
    CameraSchema,
    ResultSchema,
    CameraNotFoundException,
    InvalidRangeException,
    RangeNotFoundException,
    ResponseException
)


class ErrorResponseSchema(Schema):
    error = fields.String(default=None)


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

    @docs(
        summary='Get the registered cameras',
        responses={
            500: {'schema': ErrorResponseSchema}
        }
    )
    @response_schema(CameraSchema(many=True))
    async def cameras(self, request: web.Request):
        cameras = []
        for camera in self.dvr.cameras.values():
            cameras.append(CameraSchema().dump(camera))
        return web.json_response(cameras)

    @docs(
        summary='Search for stored video data',
        responses={
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
            return web.json_response(ErrorResponseSchema().dump({'error': str(ex)}), status=404)
        except InvalidRangeException as ex:
            return web.json_response(ErrorResponseSchema().dump({'error': str(ex)}), status=409)
        except RangeNotFoundException as ex:
            return web.json_response(ErrorResponseSchema().dump({'error': str(ex)}), status=416)
        except ResponseException as ex:
            return web.json_response(ErrorResponseSchema().dump({'error': str(ex)}), status=500)

    @docs(
        summary='Download video data',
        responses={
            200: {'description': 'Video data'},
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
                camera,
                result.start_time.strftime('%Y%m%d%H%M%S'),
                result.end_time.strftime('%Y%m%d%H%M%S')
            )

            response = web.StreamResponse()
            await response.prepare(request)
            for percentage in ffmpeg.download_rtsp(result.rtsp_uri, result.end_time - result.start_time, filename):
                line = '{}\n'.format(percentage)
                await response.write(line.encode('utf-8'))

            await response.write_eof(b'')
        except CameraNotFoundException as ex:
            return web.json_response(ErrorResponseSchema().dump({'error': str(ex)}), status=404)
        except InvalidRangeException as ex:
            return web.json_response(ErrorResponseSchema().dump({'error': str(ex)}), status=409)
        except RangeNotFoundException as ex:
            return web.json_response(ErrorResponseSchema().dump({'error': str(ex)}), status=416)
        except ResponseException as ex:
            return web.json_response(ErrorResponseSchema().dump({'error': str(ex)}), status=500)


if __name__ == '__main__':
    frankamera = Frankamera()

    app = web.Application()
    app.add_routes([
        web.get('/cameras', frankamera.cameras),
        web.post('/search', frankamera.search),
        web.post('/download', frankamera.download),
    ])

    setup_aiohttp_apispec(app, title='Frankamera', version='1', url='/api/docs/swagger.json', swagger_path='/')

    web.run_app(app)
