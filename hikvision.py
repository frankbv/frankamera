from datetime import datetime
from hikvisionapi import Client
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs
import uuid
import xmltodict


class HikvisionException(Exception):
    def __init__(self, message: str, response=None):
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


class SearchResult(object):
    def __init__(self, camera: Camera, track_id: int, start_time: datetime, end_time: datetime, content_type: str,
                 playback_uri: str):
        self._camera = camera
        self._track_id = track_id
        self._start_time = start_time
        self._end_time = end_time
        self._content_type = content_type
        self._playback_uri = playback_uri
        self._filename = None
        self._filesize = None

        parsed_url = urlparse(self._playback_uri)
        parsed_qs = parse_qs(parsed_url.query or '')

        for pair in parsed_qs:
            if pair[0] == 'name':
                self._filename = pair[1]
            elif pair[0] == 'size':
                self._filesize = int(pair[1])

    @property
    def camera(self) -> Camera:
        return self._camera

    @property
    def track_id(self) -> int:
        return self._track_id

    @property
    def start_time(self) -> datetime:
        return self._start_time

    @property
    def end_time(self) -> datetime:
        return self._end_time

    @property
    def content_type(self) -> str:
        return self._content_type

    @property
    def playback_uri(self) -> str:
        return self._playback_uri

    @property
    def filename(self) -> Optional[str]:
        return self._filename

    @property
    def filesize(self) -> Optional[int]:
        return self._filesize


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
            raise HikvisionException('Invalid response while fetching cameras', response)

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
            raise HikvisionException('Invalid response while fetching camera statuses', response)

        for status in response['InputProxyChannelStatusList']['InputProxyChannelStatus']:
            camera_id = int(status['id'])
            self._cameras[camera_id].status = status['online'] == 'true'
            if 'streamingProxyChannelIdList' in status and status['streamingProxyChannelIdList'] is not None:
                for channel in status['streamingProxyChannelIdList']['streamingProxyChannelId']:
                    self._cameras[camera_id].add_channel(int(channel))

    def search(self, camera: Camera, start_time: datetime, end_time: datetime) -> List[SearchResult]:
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
            raise HikvisionException('Invalid response while searching', response)

        results = []
        if int(response['CMSearchResult']['numOfMatches']) > 0:
            items = response['CMSearchResult']['matchList']['searchMatchItem']
            if isinstance(items, dict):
                items = [items]

            for result in items:
                results.append(
                    SearchResult(
                        camera,
                        int(result['trackID']),
                        datetime.strptime(result['timeSpan']['startTime'], '%Y-%m-%dT%H:%M:%SZ'),
                        datetime.strptime(result['timeSpan']['endTime'], '%Y-%m-%dT%H:%M:%SZ'),
                        result['mediaSegmentDescriptor']['contentType'],
                        result['mediaSegmentDescriptor']['playbackURI']
                    )
                )

        return results

    def download(self, result: SearchResult, destination_path: str) -> Tuple[int, int]:
        # This request can't be done using a method chain (self.ContentMgmt.download(params=...)), because that would
        # encode the playbackURI and Hikvision doesn't like that. Also, the playbackURI seems to be some kind of ID of
        # the file, changing the starttime or endtime parameters causes Hikvision to complain or to just download the
        # entire file that matches the name parameter.
        response = self.opaque_request(
            'get',
            '{}/ISAPI/ContentMgmt/download?playbackURI={}'.format(self.host, result.playback_uri)
        )

        expected_size = int(response.headers.get('content-length', result.filesize))
        downloaded_size = 0

        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=4096):
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    yield downloaded_size, expected_size
