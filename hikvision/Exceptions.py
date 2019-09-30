from datetime import datetime
from .Camera import Camera


class ResponseException(Exception):
    def __init__(self, message: str, response):
        super().__init__(message)
        self._response = response

    def __str__(self):
        return '{}\nResponse: {}'.format(super(), self._response)


class CameraNotFoundException(Exception):
    def __init__(self, camera_id: int):
        super().__init__()
        self._camera_id = camera_id

    def __str__(self):
        return 'Camera {} not found'.format(self._camera_id)


class RangeNotFoundException(Exception):
    def __init__(self, camera: Camera, start_time: datetime, end_time: datetime):
        super().__init__()
        self._camera = camera
        self._start_time = start_time
        self._end_time = end_time

    def __str__(self):
        return 'No data found from {} until {} for camera {}'.format(self._start_time, self._end_time, self._camera)


class InvalidRangeException(Exception):
    def __init__(self, start_time: datetime, end_time: datetime):
        super().__init__()
        self._start_time = start_time
        self._end_time = end_time

    def __str__(self):
        return 'Start ({}) must be less than the end ({})'.format(self._start_time, self._end_time)
