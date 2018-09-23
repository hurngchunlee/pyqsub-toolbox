#!/usr/bin/env python

import dill as _p
import os
import os.path
import inspect
import getpass
import socket

_batchId = 0

class InputError(ValueError):
    """ Generic input error extending the system's ValueError class """
    def __init__(self, reason):
        ValueError.__init__(self)
        self._reason = reason
    def __repr__(self):
        return self._reason
    def __str__(self):
        return self.__repr__()

def _dumpSession(namePrefix=None):
    """ Dump session variables to a pickle file """
    sessId = ''
    if namePrefix:
        sessId = '%s_%s_b%d' % (namePrefix, os.getpid(), _batchId)
    else:
        sessId = '%s_%s_%s_b%d' % (getpass.getuser(), socket.gethostname(), os.getpid(), _batchId)
    pathPkl = os.path.join(os.getcwd(), '%s.pkl' % sessId)
    _p.dump_session(filename=pathPkl)
    return sessId, pathPkl

def _dumpJobData(sessId, jobSeqId, *jobData):
    """ Dump job data to a pickle file """
    jobName = '%s_j%d' % (sessId, jobSeqId)
    pathPkl = os.path.join(os.getcwd(), '%s.input' % jobName)
    try:
        f = open(pathPkl, 'wb')
        _p.dump(jobData, f)
    except e:
        raise e
    finally:
        if f:
            f.close()
    return jobName, pathPkl

def _prepareJobScript():
    """ Prepare Torque job script """
    return

def _validateInput(func, *vargs):
    """ Validate input function and its input argument list for evaluation """
    _spect = inspect.getargspec(func)
    if len(_spect.args) != len(vargs):
        raise InputError('Number of arguments not matching the function: %s' % repr(func)) 
    if len(vargs) > 1 and type(vargs[0]) is type([]):
        for v in vargs:
            if len(v) != len(vargs[0]):
                raise InputError("Unequal size between arguments for the evaluation.")
    return True

def feval(func, *vargs):
    """
    feval evaluates function with a given list of arguments.
    It is equivalent to MATLAB's feval function.
    """
    _validateInput(func, *vargs)
    return func(*vargs)

def cellfun(func, *vargs):
    """
    cellfun applies function to each element in the vargs array.
    It is equivalent to MATLAB's cellfun function.
    """
    _validateInput(func, *vargs)
    out = []
    for i in xrange(len(vargs[0])):
        vlist = map(lambda x:x[i], vargs)
        out.append( func(*vlist) )
    return out

def qsubfeval(func, *vargs, **kwargs):
    """ Run the feval via a job on the Torque cluster. """
    _validateInput(func, *vargs)
    _sessId, _sessPathPkl = _dumpSession( None if 'name' not in kwargs.keys() else kwargs['name'] )
    _jobName, _jobPathPkl = _dumpJobData(_sessId, 1, *vargs)

def qsubcellfun(func, *vargs, **kwargs):
    """ Run the cellfun via distributed jobs on the Torque cluster. """
    _validateInput(func, *vargs)

    # TODO: dump session variables

    out = []
    for i in xrange(len(vargs[0])):
        vlist = map(lambda x:x[i], vargs)

        # TODO: prepare jobs
        # TODO: submit jobs
        # TODO: monitor jobs until they all finish
        # TODO: load output into the output array 

    return out
