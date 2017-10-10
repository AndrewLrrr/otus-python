import gzip
import os
import unittest
from struct import unpack

import pb
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
        message_counter = 0
        bytes_written = pb.deviceapps_xwrite_pb(self.deviceapps, TEST_FILE)
        self.assertTrue(bytes_written > 0)
        with gzip.open(TEST_FILE, 'r') as fi:
            header_bites = []
            message_bytes = []
            message_length = 0
            for bytes in fi.readlines():
                for byte in bytes:
                    if message_length > 0:
                        message_bytes.append(byte)
                        message_length -= 1
                    else:
                        if len(header_bites) != header_size:
                            header_bites.append(byte)
                        else:
                            magic, device_type, message_length = unpack('Ihh', ''.join(header_bites))
                            if magic == MAGIC:
                                message_counter += 1
                            self.assertEqual(magic, MAGIC)
                            self.assertEqual(device_type, DEVICE_APPS_TYPE)
            self.assertEqual(message_counter, len(self.deviceapps))

    @unittest.skip("Optional problem")
    def test_read(self):
        pb.deviceapps_xwrite_pb(self.deviceapps, TEST_FILE)
        for i, d in pb.deviceapps_xread_pb(TEST_FILE):
            self.assertEqual(d, self.deviceapps[i])
