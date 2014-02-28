from pkg_resources import resource_filename

import h5py
import numpy
from webob.request import Request

from pydap.handlers.hdf5 import Hdf5Data

def test_can_instantiate(hdf5_dst):
    var = Hdf5Data(hdf5_dst)
    assert var.shape == (10, 10, 10)
    assert var.iter

def test_can_iterate_on_unsliced(hdf5data_iterable):
    for data in hdf5data_iterable:
        pass
    assert True

def test_can_iterate_on_sliced_major(hdf5data_instance_3d):
    i = 0
    for data in hdf5data_instance_3d[5:10,:,:]:
        i += 1
    assert i == 5

def test_can_iterate_on_sliced_minor(hdf5data_instance_3d):
    i = 0
    for data in hdf5data_instance_3d[:,1:2,3:4]:
        i += 1
    assert i == 10

def test_can_iterate_on_sliced_major_minor(hdf5data_instance_3d):
    i = 0
    for data in hdf5data_instance_3d[0:2,3:4,5:6]:
        i += 1
    assert i == 2
        
def test_shape_of_unsliced_3d(hdf5data_instance_3d):
    x = hdf5data_instance_3d
    assert x.shape == (10, 10, 10)

def test_shape_of_sliced_3d(hdf5data_instance_3d):
    x = hdf5data_instance_3d
    assert x[5:10,:,:].shape == (5, 10, 10)

def test_shape_of_unsliced_1d(hdf5data_instance_1d):
    x = hdf5data_instance_1d
    assert x.shape == (10,)

def test_shape_of_sliced_1d(hdf5data_instance_1d):
    x = hdf5data_instance_1d
    assert x[5:10].shape == (5,)

# Unless 1d variables are of unlimited dimensions, you should get all of their output on the first iteration in a numpy array
def test_1d_iteration(hdf5data_instance_1d):
    x = hdf5data_instance_1d
    for i in iter(x):
        if hdf5data_instance_1d.var.maxshape == (None,):
            assert type(i) == numpy.float64
        else:
            assert type(i) == numpy.ndarray
            assert len(i) == len(hdf5data_instance_1d.var)

def test_can_slice_a_sliced_dataset(hdf5data_instance_3d):
    x = hdf5data_instance_3d
    subset = x[5:10,:,:][1:2,:,:]
    assert subset.shape == (1, 10, 10)
    
def test_the_bounds():
    test_bounds = resource_filename('pydap.handlers.hdf5', 'data/bounds.h5')
    from pydap.handlers.hdf5 import HDF5Handler
    app = HDF5Handler(test_bounds)
    req = Request.blank('/bounds.nc.ascii?climatology_bounds')
    resp = req.get_response(app)
    assert resp.status == '200 OK'
    print resp.body
