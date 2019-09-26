from clint.textui import progress
from datetime import datetime, timedelta
import json
import sys

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
        print('{}. {}'.format(camera_id, camera.name))

    camera_id = int(input('Choose camera to download from: ').strip() or 0)
    if camera_id not in dvr.cameras:
        print('Camera {} does not exist'.format(camera_id))
        sys.exit(1)

    start_time = datetime.strptime(input('Start date and time (yyyy-mm-ss hh:mm:ss): ').strip(), '%Y-%m-%d %H:%M:%S')
    end_time = start_time + timedelta(seconds=int(input('Length in seconds (default: 30): ').strip() or 30))
    camera = dvr.cameras[camera_id]

    results = dvr.search(camera, start_time, end_time)
    if len(results) == 0:
        print('No recordings found for camera {}'.format(camera.name))
        sys.exit(1)

    print('Downloading {} recordings from {} to {} from camera {}'
          .format(len(results), results[0].start_time, results[-1].end_time, camera.name))

    for result in results:
        filename = '{}_{}_{}.mp4'.format(
            result.camera.name,
            result.start_time.strftime('%Y%m%d%H%M%S'),
            result.end_time.strftime('%Y%m%d%H%M%S'),
        )

        bar = progress.Bar()
        for downloaded_size, expected_size in dvr.download(result, filename):
            bar.show(downloaded_size, expected_size)
except KeyboardInterrupt:
    sys.exit(0)
