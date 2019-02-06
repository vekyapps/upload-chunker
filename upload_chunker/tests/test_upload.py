import sys
import os

from os.path import dirname as up
two_up = up(up(up(__file__)))


print(two_up)
sys.path.append(two_up)

from upload_chunker import ChunkedUploader

import unittest

def fun(x):
    return x + 11

class MyTest(unittest.TestCase):
    def test(self):
        self.assertEqual(fun(5), 4)

if __name__=='__main__':
    m=MyTest()
