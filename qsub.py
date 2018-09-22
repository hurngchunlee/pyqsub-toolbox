#!/usr/bin/env python

import dill as _p 
import time as _t
import inspect as _inspect

class InputError(ValueError):
    """ Generic input error extending the system's ValueError class """
    def __init__(self, reason):
        ValueError.__init__(self)
        self._reason = reason
    def __repr__(self):
        return self._reason
    def __str__(self):
        return self.__repr__()

def _dumpSession():
    """ Dump session variables to a pickle file """
    sessId = 'sess-' % _t.time()
    sessPath = path.join(os.getcwd(), sessId)
    _p.dump_session(filename=sessPath)
    return sessPath

def _prepareJobs():
    """ Prepare Torque job content """
    return

def _validateInput(func, *vargs):
    """ Validate input function and its input argument list for evaluation """
    _spect = _inspect.getargspec(func)
    if len(_spect.args) != len(vargs):
        raise InputError('Number of arguments not matching the function: %s' % repr(func)) 
    if len(vargs) > 1:
        for v in vargs:
            if len(v) != len(vargs[0]):
                raise InputError("Unequal size between arguments for the evaluation.")
    return True

def feval(func, *vargs):
    """ Simular to MATLAB's feval function. """
    _validateInput(func, *vargs)
    out = []
    for i in xrange(len(vargs[0])):
        vlist = map(lambda x:x[i], vargs)
        out.append( func(*vlist) )
    return out

def qsubfeval(func, *vargs, **kwargs):
    """ Run the feval via distributed jobs on the Torque cluster. """
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
