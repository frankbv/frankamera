from datetime import datetime, timedelta
from hikvisionapi import Client
from requests.exceptions import RequestException as BaseRequestException
from typing import Dict, Optional
from tzlocal import get_localzone
from urllib.parse import urlparse, urlunparse, urlencode, ParseResult
import uuid
import xmltodict

from .Camera import Camera
from .Exceptions import (
    CameraNotFoundException,
    InvalidRangeException,
    RangeNotFoundException,
    RequestException,
    ResponseException
)
from .Result import Result

CAMERA_REFRESH_INTERVAL = timedelta(minutes=15)


class Hikvision(object):
    def __init__(
            self,
            base_url: str,
            username: str,
            password: str
    ):
        self._base_url = base_url
        self._username = username
        self._password = password
        self._client = None
        self._cameras = {}
        self._last_camera_refresh = datetime.utcfromtimestamp(0)

    def _get_client(self) -> Client:
        if self._client is None:
            self._client = Client(self._base_url, self._username, self._password)
        return self._client

    @property
    def cameras(self) -> Dict[int, Camera]:
        self.refresh_cameras()
        return self._cameras

    def get_camera_by_id(self, camera_id: int) -> Camera:
        if camera_id not in self.cameras:
            raise CameraNotFoundException(camera_id)
        return self.cameras[camera_id]

    def refresh_cameras(self):
        if self._last_camera_refresh + CAMERA_REFRESH_INTERVAL > datetime.utcnow():
            return

        try:
            response = self._get_client().ContentMgmt.InputProxy.channels(method='get')
            if 'InputProxyChannelList' not in response or 'InputProxyChannel' not in response['InputProxyChannelList']:
                raise ResponseException('Invalid response while fetching cameras', response)
        except BaseRequestException as ex:
            raise RequestException('Could not connect to Hikvision DVR: {}'.format(ex))

        for camera in response['InputProxyChannelList']['InputProxyChannel']:
            camera_id = int(camera['id'])
            self._cameras[camera_id] = Camera(
                camera_id,
                camera['name'],
                camera['sourceInputPortDescriptor']['ipAddress']
            )

        try:
            response = self._get_client().ContentMgmt.InputProxy.channels.status(method='get')
            if 'InputProxyChannelStatusList' not in response \
                    or 'InputProxyChannelStatus' not in response['InputProxyChannelStatusList']:
                raise ResponseException('Invalid response while fetching camera statuses', response)
        except BaseRequestException as ex:
            raise RequestException('Could not connect to Hikvision DVR: {}'.format(ex))

        for status in response['InputProxyChannelStatusList']['InputProxyChannelStatus']:
            camera_id = int(status['id'])
            self._cameras[camera_id].status = status['online'] == 'true'
            if 'streamingProxyChannelIdList' in status and status['streamingProxyChannelIdList'] is not None:
                for channel in status['streamingProxyChannelIdList']['streamingProxyChannelId']:
                    self._cameras[camera_id].add_channel(int(channel))

        self._last_camera_refresh = datetime.utcnow()

    def search(self, camera: Camera, start_time: datetime, end_time: datetime) -> Result:
        tz = get_localzone()

        # If no timezone is set, assume the datetime is in the local timezone
        if start_time.utcoffset() is None:
            start_time = tz.localize(start_time)
        if end_time.utcoffset() is None:
            end_time = tz.localize(end_time)

        # Make sure we work with datetimes that are in the local timezone
        start_time = tz.normalize(start_time.astimezone(tz))
        end_time = tz.normalize(end_time.astimezone(tz))

        if end_time <= start_time:
            raise InvalidRangeException(start_time, end_time)

        data = xmltodict.unparse({
            'CMSearchDescription': {
                'searchID': uuid.uuid4(),
                'trackList': [{
                    'trackID': camera.channels[0]
                }],
                'timeSpanList': [{
                    'timeSpan': {
                        'startTime': start_time.isoformat(),
                        'endTime': end_time.isoformat()
                    }
                }],
                'maxResults': 50,
                'searchResultPosition': 0,
                'metadataList': [{
                    'metadataDescriptor': '//recordType.meta.std-cgi.com',
                }]
            }
        })

        headers = {'content-type': 'application/xml; charset="UTF-8"'}
        try:
            response = self._get_client().ContentMgmt.search(method='post', data=data, headers=headers)
            if 'CMSearchResult' not in response \
                    or 'responseStatus' not in response['CMSearchResult'] \
                    or response['CMSearchResult']['responseStatus'] != 'true' \
                    or 'numOfMatches' not in response['CMSearchResult']:
                raise ResponseException('Invalid response while searching', response)
        except BaseRequestException as ex:
            raise RequestException('Could not connect to Hikvision DVR: {}'.format(ex))

        result = None
        if int(response['CMSearchResult']['numOfMatches']) > 0:
            items = response['CMSearchResult']['matchList']['searchMatchItem']
            if isinstance(items, dict):
                items = [items]

            # These timestamps are not actually UTC, but the local time on the DVR
            result = {
                'start_time': tz.normalize(
                    tz.localize(datetime.strptime(items[0]['timeSpan']['startTime'], '%Y-%m-%dT%H:%M:%SZ'))
                ),
                'end_time': tz.normalize(
                    tz.localize(datetime.strptime(items[-1]['timeSpan']['endTime'], '%Y-%m-%dT%H:%M:%SZ'))
                ),
                'rtsp_uri': items[0]['mediaSegmentDescriptor']['playbackURI']
            }

        if result is None:
            raise RangeNotFoundException(camera, start_time, end_time)

        if result['start_time'] > start_time:
            start_time = result['start_time']

        if result['end_time'] < end_time:
            end_time = result['end_time']

        parsed_uri = urlparse(result['rtsp_uri'])
        rtsp_uri = urlunparse(
            ParseResult(
                parsed_uri.scheme,
                parsed_uri.netloc,
                parsed_uri.path,
                '',
                urlencode({'starttime': start_time.strftime('%Y%m%dT%H%M%SZ')}),
                ''
            )
        )

        return Result(camera.id, start_time, end_time, rtsp_uri)
