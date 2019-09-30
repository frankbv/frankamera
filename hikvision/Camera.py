from dataclasses import dataclass
from marshmallow import Schema, fields


class CameraSchema(Schema):
    id = fields.Integer()
    name = fields.String()
    ip_address = fields.String()
    status = fields.Boolean()


@dataclass
class Camera(object):
    def __init__(self, camera_id: int, name: str, ip_address: str):
        self._id = camera_id
        self._name = name
        self._ip_address = ip_address
        self._status = False
        self._channels = []

    @property
    def id(self) -> int:
        return self._id

    @property
    def name(self) -> str:
        return self._name

    @property
    def ip_address(self) -> str:
        return self._ip_address

    @property
    def status(self) -> bool:
        return self._status

    @status.setter
    def status(self, status: bool):
        self._status = status

    @property
    def channels(self):
        return self._channels

    def add_channel(self, channel: int):
        self._channels.append(channel)

    def __str__(self):
        return self.name



