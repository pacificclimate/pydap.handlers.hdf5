import os
import re
import time
from stat import ST_MTIME
from email.utils import formatdate
from warnings import warn

import numpy as np
import h5py
from pupynere import REVERSE

from pydap.model import DatasetType, StructureType, SequenceType, GridType, BaseType
from pydap.handlers.lib import BaseHandler
from pydap.exceptions import OpenFileError

from pdb import set_trace

class HDF5Handler(BaseHandler):

    extensions = re.compile(r"^.*\.h(df)?[45]?$", re.IGNORECASE)

    def __init__(self, filepath):
        BaseHandler.__init__(self)

        try:
            self.fp = h5py.File(filepath, 'r')
        except Exception, exc:
            message = 'Unable to open file %s: %s' % (filepath, exc)
            raise OpenFileError(message)

        self.additional_headers.append(
                ('Last-modified', (formatdate(time.mktime(time.localtime(os.stat(filepath)[ST_MTIME]))))))

        attrs = {'NC_GLOBAL': []}

        unlim = find_unlimited(self.fp)
        if len(unlim) > 1:
            raise Exception("Found %d unlimited dimensions %s, but DAP supports no more than one.")
        elif len(unlim) == 1:
            attrs.update({'DODS_EXTRA': {'Unlimited_Dimension': unlim.pop()}})

        # build dataset
        name = os.path.split(filepath)[1]
        self.dataset = DatasetType(name, attributes=attrs)

        def is_gridded(dst):
            return sum([len(dim) for dim in dst.dims]) > 0

        def add_variables(dataset, h5, level=0):
            assert type(h5) in (h5py.File, h5py.Group, h5py.Dataset)
            name = h5.name.lstrip('/')
            attrs = {}
            for key in h5.attrs.keys():
                try:
                    REVERSE(h5.attrs.get(key).dtype) # This will raise Exception of the type is not convertable
                    attrs[key] = h5.attrs.get(key)
                except:
                    print "Failed on", key
    
            # struct
            if type(h5) in (h5py.File, h5py.Group):
                foo = StructureType(name, attributes=attrs)
                name = foo.name
                dataset[name] = foo
                for bar in h5.values():
                    add_variables(dataset[name], bar, level+1)
                return

            # Recursion base cases
            rank = len(h5.shape)
            # basetype
            if rank == 0:
                dataset[name] = BaseType(name, data=Hdf5Data(h5), dimensions=(), attributes=attrs)
            # sequence?
            #elif rank == 1:
            #    dataset[name] = SequenceType(name, data=h5, attributes=h5.attrs)
            # grid
            elif is_gridded(h5):
                parent = dataset[name] = GridType(name, attributes=attrs)
                dims = tuple([ d.keys()[0] for d in h5.dims ])
                parent[name] = BaseType(name, data=Hdf5Data(h5), dimensions=dims, attributes=attrs) # Add the main variable
                for dim in h5.dims: # and all of the dimensions
                    add_variables(parent, dim[0], level+1) # Why would dims have more than one h5py.Dataset?
            # BaseType
            else:
                dataset[name] = BaseType(name, data=Hdf5Data(h5), attributes=attrs)

        for varname in self.fp:
            add_variables(self.dataset, self.fp[varname])

    def close(self):
        self.fp.close()
        
def find_unlimited(h5):
    'Recursively construct a set of names for unlimited dimensions in an hdf dataset'
    rv = set()
    if type(h5) == h5py.Dataset:
        try:
            dims = tuple([ d.keys()[0] for d in h5.dims ])
        except:
            return set()
        maxshape = h5.maxshape
        rv = [ dimname for dimname, length in zip(dims, maxshape) if not length ] # length is None for unlimited dimensions
        return set(rv)
    for child in h5.values():
        rv.update(find_unlimited(child))
    return rv

def process_attrs(attrs):
    rv = {}
    for key in attrs.keys():
        val = attrs.get(key)
        try:
            REVERSE(val.dtype) # This will raise Exception of the type is not convertable
            rv[key] = val
        except:
            warn("Failed to convert attribute", key + ":" + val)

class Hdf5Data(object):
    """
    A wrapper for Hdf5 variables, ensuring support for iteration and the dtype
    property
    """
    # FIXME: This is ridiculously slow
    def __init__(self, var, index=[]):
        self.var = var
        self.index = index

        # Set up the iterartor
        x = self.var
        for i in self.index:
            x = x.__getitem__(i)
        try:
            self.iter = iter(x)
        except TypeError:
            self.iter = x

    # FIXME: this is horrible
    def __getitem__(self, index):
        #if not self.index:
        print "Called __getitem__ on dataset", self.var.name, "with index", index, "total indexes", self.index + [index]
        # FIXME: Check if the indexes are actually valid!
        # FIXME: Apply the indexes to the correct dimensions!
        return Hdf5Data(self.var, self.index + [index])

    def __iter__(self):
        return self

    def next(self):
        try:
            return self.iter.next()
        except StopIteration:
            self.iter = iter(self.var)
            raise
    def __len__(self): return self.var.shape[0]

    @property
    def dtype(self):
        return self.var.dtype

    @property
    def shape(self):
        myshape = self.var.shape
        for slice_ in self.index:
            myshape = sliced_shape(slice_, myshape)
        return myshape

def sliced_shape(slice_, shape_):
    if not isinstance(slice_, tuple): slice_ = (slice_,)
    assert len(slice_) == len(shape_)
    rv = [ len(range(s.start, s.stop, s.step)) for s in slice_ ]
    print 'returning', rv
    return tuple(rv)

if __name__ == "__main__":
    import sys
    from werkzeug.serving import run_simple

    application = HDF5Handler(sys.argv[1])
    run_simple('localhost', 8002, application, use_reloader=True)
