import os
from tempfile import NamedTemporaryFile
from pkg_resources import resource_filename

import pytest
import numpy.random
import h5py
from pydap.handlers.hdf5 import Hdf5Data

test_h5 = resource_filename('pydap.handlers.hdf5', 'data/test.h5')

@pytest.fixture(scope="function", params=['/tasmax', '/tasmin', '/pr'])
def hdf5data_instance_3d(request):
    f = h5py.File(test_h5, 'r')
    dst = f[request.param]
    return Hdf5Data(dst)

@pytest.fixture(scope="module", params=['/lat', '/lon', '/time'])
def hdf5data_instance_1d(request):
    f = h5py.File(test_h5, 'r')
    dst = f[request.param]
    return Hdf5Data(dst)

# _All_ the variables should be iterable
@pytest.fixture(scope="module", params=['/tasmax', '/tasmin', '/pr', '/lat', '/lon', '/time'])
def hdf5data_iterable(request):
    f = h5py.File(test_h5, 'r')
    dst = f[request.param]
    return Hdf5Data(dst)

@pytest.fixture(scope="function")
def hdf5_dst(request):
    f = NamedTemporaryFile()
    hf = h5py.File(f.name, driver='core', backing_store=False)
    group = hf.create_group('foo')
    dst = group.create_dataset('bar', (10, 10, 10), '=f8', maxshape=(None, 10, 10))
    dst[:,:,:] = numpy.random.rand(10, 10, 10)

    def fin():
        hf.close()
        os.remove(f.name)
    request.addfinalizer(fin)
    
    return dst
