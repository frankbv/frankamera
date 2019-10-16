from datetime import datetime, timedelta, timezone
import json
from marshmallow import Schema, fields
from multiprocessing import Manager, Pool
import os
import re
import select
import shlex
from shutil import move
import signal
import subprocess
from typing import Dict, List, Optional
from urllib.parse import urlparse, urlunparse, ParseResult
from uuid import uuid4

PROGRESS_RE = re.compile(r'\s*(frame|fps|q|size|time|bitrate|speed)\s*=\s*(\S+)\s*')


class JobSchema(Schema):
    job_id = fields.String()
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

    def download(self, rtsp_uri: str, start_time: datetime, end_time: datetime, filename: str) -> str:
        job_id = str(uuid4())

        spool_dir = os.path.join(self._spool_path, job_id)
        os.makedirs(spool_dir, exist_ok=True)

        self._jobs[job_id] = self._manager.dict({})
        FFmpeg._job_update(
            self._jobs[job_id],
            job_id=job_id,
            filename=filename,
            start_time=start_time,
            end_time=end_time,
            spool_path=spool_dir,
            rtsp_uri=rtsp_uri,
            storage_path=os.path.join(self._storage_path, job_id),
            status=FFmpeg.PENDING,
            progress=0,
            requested_at=datetime.now(tz=timezone.utc),
        )

        self._pool.apply_async(
            FFmpeg._download_process,
            (self._jobs[job_id], self._username, self._password),
            callback=self._done,
            error_callback=self._error
        )

        return job_id

    def _done(self, job_id: str):
        if job_id in self._jobs[job_id]:
            callback = self._jobs[job_id]
            del self._jobs[job_id]

    def _error(self, ex):
        import traceback

        if isinstance(ex, JobException):
            del self._jobs[ex.job_id]

            print('Error in job {}: {}'.format(ex.job_id, str(ex)))
            traceback.print_tb(ex.__traceback__)
            print('Parent exception:')
            traceback.print_tb(ex.parent_exception.__traceback__)
        else:
            print('Error: {}'.format(str(ex)))
            traceback.print_tb(ex.__traceback__)

    @staticmethod
    def _job_update(job: Dict, **kwargs):
        job.update(kwargs)
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
                uri = job['uri']

            spool_file = os.path.join(job['spool_path'], job['filename'])

            ffmpeg_cmd = shlex.split(
                'ffmpeg -hide_banner -y -rtsp_transport tcp -rtsp_flags prefer_tcp -i "{}" -vcodec copy -an "{}"'
                .format(uri, spool_file)
            )

            time = timedelta()
            duration = job['end_time'] - job['start_time']

            (read_pipe, write_pipe) = os.pipe()
            ffmpeg = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.DEVNULL, stderr=write_pipe)

            FFmpeg._job_update(job, status=FFmpeg.RUNNING, started_at=datetime.now(tz=timezone.utc))

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

                if time.total_seconds() > duration.total_seconds():
                    if ffmpeg.poll() is None:
                        ffmpeg.terminate()

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
    def _initialize():
        # Prevent a keyboard interrupt from terminating a Pool process, this is handled by the destructor of this class
        signal.signal(signal.SIGINT, signal.SIG_IGN)
