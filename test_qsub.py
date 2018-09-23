#!/usr/bin/env python

import unittest
import dill
import sys 
import os.path
from qsub import _dumpSession, _dumpJobData

class TestRuntimeDataPickle(unittest.TestCase):

    def setUp(self):

        # dump the objects in the __main__ session.
        # it includes our 'add' function
        sesId, self.__class__.sesPathPkl = _dumpSession(None)

        # dump the data to be used in the test case for calling
        # out 'add' function
        data = (1,2)
        jobId, self.__class__.jobPathPkl = _dumpJobData(sesId, 1, *data)

    def test_loadRuntimeDataAndRun(self):
        """
        Tests the pickled function and data can be loaded and run correctly.
        """
        # ensure the 'add' function from the test's __main__ session is removed and
        # not available to this test case.
        del globals()['add']
        self.assertFalse( 'add' in globals().keys() )

        # load session data and test if the 'add' function is again avaliable after
        # data is loaded.
        dill.load_session(self.__class__.sesPathPkl)
        self.assertTrue( 'add' in globals().keys() )

        # load data for the 'add' function, and test if we get expected result after
        # running the 'add' function with the loaded data. 
        with open(self.__class__.jobPathPkl, 'rb') as f:
            data = dill.load(f)
            self.assertTrue( add(*data) == 3 )

if __name__ == '__main__':
    # user defined function
    def add(a,b):
        return a+b

    unittest.main()
