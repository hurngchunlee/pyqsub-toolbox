#!/usr/bin/env python

import dill as _p
import os
import os.path
import inspect
import getpass
import socket
from string import Template

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
    except Exception, e:
        raise e
    finally:
        if f:
            f.close()
    return jobName, pathPkl

def _prepareJobScript(sesId, sesPathPkl, jobName, jobInputPkl, func):
    """ Prepare Torque job script """

    jobOutputPkl = '%s.output' % jobInputPkl.rstrip('.input') 
    jobScriptPath = '%s.py' % jobInputPkl.rstrip('.input')

    jobParams = {
        'SES_ID': sesId,
        'SES_PATH_PKL': sesPathPkl,
        'JOB_NAME': jobName,
        'JOB_WDIR': os.getcwd(),
        'JOB_INPUT_PKL': jobInputPkl,
        'JOB_OUTPUT_PKL': jobOutputPkl,
        'FUNC_NAME': func.__name__
    }

    s = Template('''#!/usr/bin/env python
#PBS -N ${JOB_NAME}
#PBS -d ${JOB_WDIR} 
import dill
import sys

ec = 0
errmsg = ''
out = None
try:
    dill.load_session(filename='${SES_PATH_PKL}')

    with open('${JOB_INPUT_PKL}', 'rb') as f:
        data = dill.load(f)
        f.close()
        out = ${FUNC_NAME}(*data)

except Exception, e:
    ec = 1
    errmsg = 'Exception: %s', e
    sys.stderr.write('Exception: %s\\n', e)

with open('${JOB_OUTPUT_PKL}', 'wb') as f:
    dill.dump({'out':out, 'ec': ec, 'errmsg': errmsg}, f)
    f.close()
''').substitute(**jobParams)

    try:
        f = open(jobScriptPath, 'w')
        f.write(s)
    except Exception, e:
        raise e
    finally:
        f.close()

    return jobScriptPath

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
    _sid, _sPathPkl = _dumpSession( None if 'name' not in kwargs.keys() else kwargs['name'] )
    _jName, _jInputPkl = _dumpJobData(_sid, 1, *vargs)
    _jScript = _prepareJobScript(_sid, _sPathPkl, _jName, _jInputPkl, func)

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
