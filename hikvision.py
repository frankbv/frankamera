from datetime import datetime, timedelta
from hikvisionapi import Client
from typing import Dict, Optional
from urllib.parse import urlparse, parse_qsl, urlunparse, urlencode, ParseResult
import uuid
import xmltodict


class ResponseException(Exception):
    def __init__(self, message: str, response):
        super().__init__(message)
        self._response = response

    @property
    def response(self):
        return self._response

    def __str__(self):
        return '{}\nResponse: {}'.format(super(), self.response)


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


class NotFoundException(Exception):
    def __init__(self, camera: Camera, start_time: datetime, duration: timedelta):
        super().__init__()
        self._camera = camera
        self._start_time = start_time
        self._duration = duration

    @property
    def camera(self):
        return self._camera

    @property
    def start_time(self):
        return self._start_time

    @property
    def duration(self):
        return self._duration

    def __str__(self):
        return 'No data found on {} for {} camera {}'.format(self.start_time, self.duration, self.camera)


class Hikvision(Client):
    def __init__(self, url: str, username: str, password: str, camera_name_mapping: Optional[Dict] = None):
        super().__init__(url, username, password)
        self._cameras = {}
        self.camera_name_mapping = camera_name_mapping or {}

    @property
    def cameras(self) -> Dict[int, Camera]:
        if not self._cameras:
            self.refresh_cameras()
        return self._cameras

    def refresh_cameras(self):
        response = self.ContentMgmt.InputProxy.channels(method='get')
        if 'InputProxyChannelList' not in response or 'InputProxyChannel' not in response['InputProxyChannelList']:
            raise ResponseException('Invalid response while fetching cameras', response)

        for camera in response['InputProxyChannelList']['InputProxyChannel']:
            camera_id = int(camera['id'])
            if camera['sourceInputPortDescriptor']['ipAddress'] in self.camera_name_mapping:
                name = self.camera_name_mapping[camera['sourceInputPortDescriptor']['ipAddress']]
            else:
                name = camera['name']

            self._cameras[camera_id] = Camera(camera_id, name, camera['sourceInputPortDescriptor']['ipAddress'])

        response = self.ContentMgmt.InputProxy.channels.status(method='get')
        if 'InputProxyChannelStatusList' not in response \
                or 'InputProxyChannelStatus' not in response['InputProxyChannelStatusList']:
            raise ResponseException('Invalid response while fetching camera statuses', response)

        for status in response['InputProxyChannelStatusList']['InputProxyChannelStatus']:
            camera_id = int(status['id'])
            self._cameras[camera_id].status = status['online'] == 'true'
            if 'streamingProxyChannelIdList' in status and status['streamingProxyChannelIdList'] is not None:
                for channel in status['streamingProxyChannelIdList']['streamingProxyChannelId']:
                    self._cameras[camera_id].add_channel(int(channel))

    def search(self, camera: Camera, start_time: datetime, duration: timedelta) -> Dict:
        end_time = start_time + duration

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
        response = self.ContentMgmt.search(method='post', data=data, headers=headers)
        if 'CMSearchResult' not in response \
                or 'responseStatus' not in response['CMSearchResult'] \
                or response['CMSearchResult']['responseStatus'] != 'true' \
                or 'numOfMatches' not in response['CMSearchResult']:
            raise ResponseException('Invalid response while searching', response)

        result = None
        if int(response['CMSearchResult']['numOfMatches']) > 0:
            items = response['CMSearchResult']['matchList']['searchMatchItem']
            if isinstance(items, dict):
                items = [items]

            # These timestamps are not actually UTC, but the local time on the DVR
            result = {
                'start_time': datetime.strptime(items[0]['timeSpan']['startTime'], '%Y-%m-%dT%H:%M:%SZ'),
                'end_time': datetime.strptime(items[-1]['timeSpan']['endTime'], '%Y-%m-%dT%H:%M:%SZ'),
                'rtsp_uri': items[0]['mediaSegmentDescriptor']['playbackURI']
            }

        if result is None:
            raise NotFoundException(camera, start_time, duration)

        if result['start_time'] > start_time:
            start_time = result['start_time']

        if result['end_time'] < end_time:
            end_time = result['end_time']

        parsed_uri = urlparse(result['rtsp_uri'])
        rtsp_uri = urlunparse(
            ParseResult(
                parsed_uri.scheme,
                '{}:{}@{}'.format(self.login, self.password, parsed_uri.netloc),
                parsed_uri.path,
                '',
                urlencode({'starttime': start_time.strftime('%Y%m%dT%H%M%SZ')}),
                ''
            )
        )

        return {
            'start_time': start_time,
            'duration': end_time - start_time,
            'rtsp_uri': rtsp_uri
        }
