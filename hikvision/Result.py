from dataclasses import dataclass
from datetime import datetime
from marshmallow import Schema, fields


class ResultSchema(Schema):
    camera_id = fields.Integer()
    start_time = fields.DateTime()
    end_time = fields.DateTime()
    rtsp_uri = fields.Url(schemes=['rtsp'])


@dataclass
class Result(object):
    def __init__(self, camera_id: int, start_time: datetime, end_time: datetime, rtsp_uri: str):
        self._camera_id = camera_id
        self._start_time = start_time
        self._end_time = end_time
        self._rtsp_uri = rtsp_uri

    @property
    def camera_id(self) -> int:
        return self._camera_id

    @property
    def start_time(self) -> datetime:
        return self._start_time

    @property
    def end_time(self) -> datetime:
        return self._end_time

    @property
    def rtsp_uri(self) -> str:
        return self._rtsp_uri
