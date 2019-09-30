from dataclasses import dataclass
from datetime import datetime, timedelta
from marshmallow import Schema, fields
from multiprocessing import Manager, Pool
import os
import re
import select
import shlex
import signal
import subprocess
from typing import Dict
from uuid import uuid4

PROGRESS_RE = re.compile(r'\s*(frame|fps|q|size|time|bitrate|speed)\s*=\s*(\S+)\s*')


class JobSchema(Schema):
    job_id = fields.UUID()
    start = fields.DateTime()
    uri = fields.Url(schemes=['rtsp'])
    duration = fields.Integer()
    status = fields.String()
    progress = fields.Float()


class JobException(Exception):
    def __init__(self, job_id: str, parent_exception: Exception):
        self._job_id = job_id
        self._parent_exception = parent_exception

    @property
    def job_id(self) -> int:
        return self._job_id

    @property
    def parent_exception(self) -> Exception:
        return self._parent_exception

    def __str__(self):
        return 'Error while processing job {}: {}'.format(self.job_id, self.parent_exception)


class FFmpeg(object):
    PENDING = 'pending'
    DONE = 'done'
    RUNNING = 'running'

    def __init__(self, workers=5, max_downloads_per_child=100):
        self._manager = Manager()
        self._pool = Pool(processes=workers, maxtasksperchild=max_downloads_per_child, initializer=self._initialize)
        self._jobs = {}

        self._progress_handlers = []
        self._done_handlers = []
        self._error_handlers = []

    def __del__(self):
        self._pool.terminate()
        self._pool.join()

    def get_jobs(self):
        return [dict(job) for job in self._jobs.values()]

    def download(self, uri: str, duration: timedelta) -> str:
        job_id = str(uuid4())

        self._jobs[job_id] = self._manager.dict({
            'job_id': job_id,
            'start': None,
            'uri': uri,
            'duration': duration.total_seconds(),
            'status': FFmpeg.PENDING,
            'progress': 0
        })

        self._pool.apply_async(
            FFmpeg._download_process,
            (self._jobs[job_id],),
            callback=self._done,
            error_callback=self._error
        )

        return job_id

    def _done(self, job_id: str):
        for handler in self._progress_handlers:
            handler(self._jobs[job_id])

    def _error(self, ex):
        for handler in self._error_handlers:
            handler(ex)

    @staticmethod
    def _download_process(job: Dict) -> str:
        try:
            ffmpeg_cmd = shlex.split(
                """
                ffmpeg -hide_banner -y -rtsp_transport tcp -rtsp_flags prefer_tcp -i "{}" -vcodec copy -an "{}"
                """.format(job['uri'], '{}.mp4'.format(job['job_id']))
            )

            time = timedelta()

            (read_pipe, write_pipe) = os.pipe()
            ffmpeg = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.DEVNULL, stderr=write_pipe)

            job['status'] = FFmpeg.RUNNING
            job['start'] = datetime.utcnow()

            while ffmpeg.poll() is None:
                fd_count = len(select.select([read_pipe], [], [], 1)[0])
                if fd_count == 1:
                    buf = os.read(read_pipe, 1024)
                    progress = dict(PROGRESS_RE.findall(str(buf)))
                    if 'frame' in progress and 'size' in progress and 'time' in progress:
                        hours, minutes, seconds = progress['time'].split(':')
                        time = timedelta(hours=int(hours), minutes=int(minutes), seconds=float(seconds))
                        job['progress'] = time.total_seconds() / max(time.total_seconds(), job['duration']) * 100

                if time.total_seconds() > job['duration']:
                    if ffmpeg.poll() is None:
                        ffmpeg.terminate()

            ffmpeg.wait()

            job['status'] = FFmpeg.DONE
            job['progress'] = 100

            return job['job_id']
        except Exception as ex:
            raise JobException(job['job_id'], ex)

    @staticmethod
    def _initialize():
        # Prevent a keyboard interrupt from terminating a Pool process, this is handled by the destructor of this class
        signal.signal(signal.SIGINT, signal.SIG_IGN)
