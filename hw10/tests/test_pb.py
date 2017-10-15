import os
import unittest
import gzip
from struct import unpack

import pb
import deviceapps_pb2 as dapps_pb2

MAGIC = 0xFFFFFFFF
DEVICE_APPS_TYPE = 1
TEST_FILE = "test.pb.gz"


class TestPB(unittest.TestCase):
    deviceapps = [
        {"device": {"type": "idfa", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7c"},
         "lat": 67.7835424444, "lon": -22.8044005471, "apps": [1, 2, 3, 4]},
        {"device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"}, "lat": 42, "lon": -42, "apps": [1, 2]},
        {"device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"}, "lat": 42, "lon": -42, "apps": []},
        {"device": {"type": "gaid", "id": "e7e1a50c0ec2747ca56cd9e1558c0d7d"}, "apps": [1]},
    ]

    def tearDown(self):
        os.remove(TEST_FILE)

    def test_write(self):
        header_size = 8
        bytes_written = pb.deviceapps_xwrite_pb(self.deviceapps, TEST_FILE)
        self.assertTrue(bytes_written > 0)
        with gzip.open(TEST_FILE, 'r') as fi:
            for deviceapp in self.deviceapps:
                magic, device_type, message_length = unpack('<Ihh', fi.read(header_size))
                self.assertEqual(magic, MAGIC)
                self.assertEqual(device_type, DEVICE_APPS_TYPE)
                message = fi.read(message_length)

                da = dapps_pb2.DeviceApps()
                da.ParseFromString(message)

                da_orig = dapps_pb2.DeviceApps()
                da_orig.device.id = deviceapp['device']['id']
                da_orig.device.type = deviceapp['device']['type']
                da_orig.apps.extend(deviceapp['apps'])
                if 'lat' in deviceapp:
                    da_orig.lat = deviceapp['lat']
                if 'lon' in deviceapp:
                    da_orig.lon = deviceapp['lon']

                self.assertEqual(da, da_orig)

    @unittest.skip("Optional problem")
    def test_read(self):
        pb.deviceapps_xwrite_pb(self.deviceapps, TEST_FILE)
        for i, d in pb.deviceapps_xread_pb(TEST_FILE):
            self.assertEqual(d, self.deviceapps[i])
