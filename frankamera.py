from clint.textui import progress
from datetime import datetime, timedelta
import json
import sys

import ffmpeg
from hikvision import Hikvision

try:
    with open('frankamera.json') as f:
        config = json.load(f)

    dvr = Hikvision(
        config['hikvision']['base_url'],
        config['hikvision']['username'],
        config['hikvision']['password'],
        config['cameras']
    )

    for camera_id, camera in dvr.cameras.items():
        print('{}. {}'.format(camera_id, camera))

    camera_id = int(input('Choose camera to download from: ').strip() or 0)
    if camera_id not in dvr.cameras:
        print('Camera {} does not exist'.format(camera_id))
        sys.exit(1)

    start_time = datetime.strptime(input('Start date and time (yyyy-mm-ss hh:mm:ss): ').strip(), '%Y-%m-%d %H:%M:%S')
    duration = timedelta(seconds=int(input('Length in seconds (default: 30): ').strip() or 30))
    camera = dvr.cameras[camera_id]

    result = dvr.search(camera, start_time, duration)

    print('Downloading recording of {} for {} from camera {}'.format(result['start_time'], result['duration'], camera))

    filename = '{}_{}_{}.mp4'.format(camera, result['start_time'].strftime('%Y%m%d%H%M%S'), result['duration'])

    bar = progress.Bar(expected_size=100)
    for percentage in ffmpeg.download_rtsp(result['rtsp_uri'], result['duration'], filename):
        bar.show(percentage)

    print()

except KeyboardInterrupt:
    sys.exit(0)
