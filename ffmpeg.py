from datetime import datetime, timedelta, timezone
import json
import logging
from marshmallow import Schema, fields
from multiprocessing import Manager, Pool
import os
import re
import requests
import select
import shlex
from shutil import move
import signal
import subprocess
from typing import Dict, List, Optional
from urllib.parse import urlparse, urlunparse, ParseResult
from uuid import uuid4

from hikvision import Camera

PROGRESS_RE = re.compile(r'\s*(frame|fps|q|size|time|bitrate|speed)\s*=\s*(\S+)\s*')


class JobSchema(Schema):
    job_id = fields.String()
    camera_id = fields.Integer()
    filename = fields.String()
    start_time = fields.DateTime()
    end_time = fields.DateTime()
    status = fields.String()
    progress = fields.Float()
    requested_at = fields.DateTime()
    started_at = fields.DateTime()
    done_at = fields.DateTime()
    error = fields.String()


class JobException(Exception):
    def __init__(self, job_id: str, parent_exception: Exception):
        self._job_id = job_id
        self._parent_exception = parent_exception

    @property
    def job_id(self) -> str:
        return self._job_id

    @property
    def parent_exception(self) -> Exception:
        return self._parent_exception

    def __str__(self):
        return 'Error while processing job {}: {}'.format(self.job_id, self.parent_exception)


class FFmpeg(object):
    DONE = 'done'
    ERROR = 'error'
    MOVING = 'moving'
    PENDING = 'pending'
    RUNNING = 'running'

    _logger = logging.getLogger('frankamera.ffmpeg')

    def __init__(
            self,
            spool_path: str,
            storage_path: str,
            username: Optional[str] = None,
            password: Optional[str] = None,
            workers: int = 5,
            max_downloads_per_child: int = 100
    ):
        self._spool_path = spool_path
        self._storage_path = storage_path
        self._username = username
        self._password = password

        self._manager = Manager()
        self._pool = Pool(processes=workers, maxtasksperchild=max_downloads_per_child, initializer=self._initialize)
        self._jobs = {}

    def __del__(self):
        self._pool.terminate()
        self._pool.join()

    def get_job_by_id(self, job_id: str) -> Optional[Dict]:
        if job_id not in self._jobs:
            try:
                job_file = os.path.join(self._spool_path, job_id, 'job.json')
                with open(job_file, 'r') as fd:
                    return JobSchema().load(json.load(fd))
            except FileNotFoundError:
                return None

        return dict(self._jobs[job_id])

    def get_all_active_jobs(self) -> List[Dict]:
        return [dict(job) for job in self._jobs.values()]

    def download(
            self,
            camera: Camera,
            rtsp_uri: str,
            start_time: datetime,
            end_time: datetime,
            filename: str,
            callback_uri: Optional[str] = None
    ) -> Dict:
        job_id = str(uuid4())

        spool_dir = os.path.join(self._spool_path, job_id)
        os.makedirs(spool_dir, exist_ok=True)

        FFmpeg.info('Pending', job=job_id)

        self._jobs[job_id] = self._manager.dict({})
        initialize_job = {
            'job_id': job_id,
            'camera_id': camera.id,
            'filename': filename,
            'start_time': start_time,
            'end_time': end_time,
            'spool_path': spool_dir,
            'rtsp_uri': rtsp_uri,
            'storage_path': os.path.join(self._storage_path, job_id),
            'status': FFmpeg.PENDING,
            'progress': 0,
            'requested_at': datetime.now(tz=timezone.utc),
            'callback_uri': callback_uri,
        }

        FFmpeg._job_update(self._jobs[job_id], **initialize_job)

        self._pool.apply_async(
            FFmpeg._download_process,
            (self._jobs[job_id], self._username, self._password),
            callback=self._done,
            error_callback=self._error
        )

        return initialize_job

    def _done(self, job_id: str):
        job = self._jobs[job_id]

        FFmpeg.info('Done {}'.format(str(job)), job=job_id)

        callback_uri = job.get('callback_uri', None)
        if callback_uri is not None:
            try:
                FFmpeg.debug('Calling callback URI {}'.format(callback_uri), job=job_id)
                response = requests.post(callback_uri, json=JobSchema().dump(job), timeout=30)
                FFmpeg.debug('Callback URI response: {} {}'.format(response.status_code, response.text), job=job_id)
                response.raise_for_status()
            except Exception as ex:
                message = 'Failed calling callback URI {}: {}'.format(callback_uri, str(ex))
                self._job_update(job, error=message)
                FFmpeg.error(message, job=job, exc_info=ex)

        del self._jobs[job_id]

    def _error(self, ex):
        if isinstance(ex, JobException):
            FFmpeg.error('Exception: {}'.format(str(ex)), job=ex.job_id, exc_info=ex)
            FFmpeg.error('Parent exception', job=ex.job_id, exc_info=ex.parent_exception)

            self._done(ex.job_id)
        else:
            FFmpeg.error('Error {}'.format(str(ex)), exc_info=ex)

    @staticmethod
    def _job_update(job: Dict, **kwargs):
        job.update(kwargs)
        FFmpeg.debug("Update: {}".format(str(kwargs)), job=job)
        with open(os.path.join(job['spool_path'], 'job.json'), 'w') as fd:
            json.dump(JobSchema().dump(job), fd)

    @staticmethod
    def _download_process(
            job: Dict,
            username: Optional[str] = None,
            password: Optional[str] = None
    ) -> str:
        try:
            if username and password:
                parsed_uri = urlparse(job['rtsp_uri'])
                uri = urlunparse(
                    ParseResult(
                        parsed_uri.scheme,
                        '{}:{}@{}'.format(username, password, parsed_uri.netloc),
                        parsed_uri.path,
                        '',
                        parsed_uri.query,
                        ''
                    )
                )
            else:
                uri = job['rtsp_uri']

            FFmpeg.debug('Getting video data from {}'.format(uri), job=job)

            spool_file = os.path.join(job['spool_path'], job['filename'])

            ffmpeg_cmd = shlex.split(
                'ffmpeg -hide_banner -y -rtsp_transport tcp -rtsp_flags prefer_tcp -i "{}" -vcodec copy -an "{}"'
                .format(uri, spool_file)
            )

            time = timedelta()
            duration = job['end_time'] - job['start_time']

            (read_pipe, write_pipe) = os.pipe()
            FFmpeg.info('Starting', job=job)
            ffmpeg = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.DEVNULL, stderr=write_pipe)

            FFmpeg._job_update(job, status=FFmpeg.RUNNING, started_at=datetime.now(tz=timezone.utc))

            stopping = False
            while ffmpeg.poll() is None:
                fd_count = len(select.select([read_pipe], [], [], 1)[0])
                if fd_count == 1:
                    buf = os.read(read_pipe, 1024)
                    progress = dict(PROGRESS_RE.findall(str(buf)))
                    if 'frame' in progress and 'size' in progress and 'time' in progress:
                        hours, minutes, seconds = progress['time'].split(':')
                        time = timedelta(hours=int(hours), minutes=int(minutes), seconds=float(seconds))
                        FFmpeg._job_update(
                            job,
                            progress=time.total_seconds() / max(time.total_seconds(), duration.total_seconds()) * 100
                        )

                if not stopping and time.total_seconds() > duration.total_seconds() and ffmpeg.poll() is None:
                    FFmpeg.debug('Stopping ffmpeg', job=job)
                    ffmpeg.terminate()
                    stopping = True

            ffmpeg.wait()

            if job['spool_path'] != job['storage_path']:
                FFmpeg._job_update(job, status=FFmpeg.MOVING, progress=100)
                os.makedirs(job['storage_path'], exist_ok=True)
                move(spool_file, os.path.join(job['storage_path'], job['filename']))

            FFmpeg._job_update(job, status=FFmpeg.DONE, progress=100, done_at=datetime.now(tz=timezone.utc))

            return job['job_id']
        except Exception as ex:
            FFmpeg._job_update(job, status=FFmpeg.ERROR, error=str(ex), done_at=datetime.now(tz=timezone.utc))
            raise JobException(job['job_id'], ex)


    @staticmethod
    def log(level: int, message: str, job: Optional = None, **kwargs):
        job_id = job if isinstance(job, str) else job['job_id']
        FFmpeg._logger.log(level, '[{}] {}'.format(job_id or 'UNKNOWN', message), **kwargs)

    @staticmethod
    def info(message: str, job: Optional = None, **kwargs):
        FFmpeg.log(logging.INFO, message, job=job, **kwargs)

    @staticmethod
    def debug(message: str, job: Optional = None, **kwargs):
        FFmpeg.log(logging.DEBUG, message, job=job, **kwargs)

    @staticmethod
    def warning(message: str, job: Optional = None, **kwargs):
        FFmpeg.log(logging.WARNING, message, job=job, **kwargs)

    @staticmethod
    def error(message: str, job: Optional = None, **kwargs):
        FFmpeg.log(logging.ERROR, message, job=job, **kwargs)

    @staticmethod
    def _initialize():
        # Prevent a keyboard interrupt from terminating a Pool process, this is handled by the destructor of this class
        signal.signal(signal.SIGINT, signal.SIG_IGN)
