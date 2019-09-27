from datetime import timedelta
import os
import re
import select
import shlex
import subprocess
from typing import Iterable


def download_rtsp(
        uri: str,
        duration: timedelta,
        destination_path: str
) -> Iterable[int]:
    ffmpeg_cmd = shlex.split(
        """
        ffmpeg -hide_banner -y -rtsp_transport tcp -rtsp_flags prefer_tcp -i "{}" -vcodec copy -an "{}"
        """.format(uri, destination_path)
    )

    progress_re = re.compile(r'\s*(frame|fps|q|size|time|bitrate|speed)\s*=\s*(\S+)\s*')

    time = timedelta()
    yield 0

    (read_pipe, write_pipe) = os.pipe()
    ffmpeg = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.DEVNULL, stderr=write_pipe)
    while ffmpeg.poll() is None:
        fd_count = len(select.select([read_pipe], [], [], 1)[0])
        if fd_count == 1:
            buf = os.read(read_pipe, 1024)
            progress = dict(progress_re.findall(str(buf)))
            if 'frame' in progress and 'size' in progress and 'time' in progress:
                hours, minutes, seconds = progress['time'].split(':')
                time = timedelta(hours=int(hours), minutes=int(minutes), seconds=float(seconds))
                yield time.total_seconds() / max(time.total_seconds(), duration.total_seconds()) * 100

        if time >= duration:
            if ffmpeg.poll() is None:
                ffmpeg.terminate()

    yield 100
    ffmpeg.wait()
