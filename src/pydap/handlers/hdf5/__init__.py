import os
import re
import time
from stat import ST_MTIME
from email.utils import formatdate
from itertools import islice, imap
import logging

import h5py
from pupynere import REVERSE

from pydap.model import DatasetType, StructureType, SequenceType, GridType, BaseType
from pydap.handlers.lib import BaseHandler
from pydap.exceptions import OpenFileError
from stack_slice import StackableSlice as ss

logger = logging.getLogger(__name__)

class HDF5Handler(BaseHandler):

    extensions = re.compile(r"^.*(\.nc4?|\.h(df)?[45]?)$", re.IGNORECASE)

    def __init__(self, filepath):
        BaseHandler.__init__(self)

        try:
            self.fp = h5py.File(filepath, 'r')
        except Exception, exc:
            message = 'Unable to open file %s: %s' % (filepath, exc)
            raise OpenFileError(message)

        self.additional_headers.append(
                ('Last-modified', (formatdate(time.mktime(time.localtime(os.stat(filepath)[ST_MTIME]))))))

        attrs = {'NC_GLOBAL': process_attrs(self.fp.attrs)}

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
            attrs = process_attrs(h5.attrs)
    
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
                dims = tuple([ d.values()[0].name.lstrip('/') for d in h5.dims ])
                logger.debug("DIMENSIONS: {}".format(dims))
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
        try:
            val = attrs.get(key) # Potentially raises TypeError: No NumPy equivalent for TypeVlenID exists
            REVERSE(val.dtype) # This will raise Exception of the type is not convertable
            rv[key] = val
        except:
            logger.warning("Failed to convert attribute " + key)
    return rv

class Hdf5Data(object):
    """
    A wrapper for Hdf5 variables, ensuring support for iteration and the dtype
    property
    """
    def __init__(self, var, slices=None):
        self.var = var
        logger.debug('%s', slices)

        rank = len(var.shape)
        assert rank > 0

        if not slices:
            self._slices = [ ss(None) for i in range(rank) ]
        else:
            assert len(slices) == rank
            self._slices = [ ss(s.start, s.stop, s.step) for s in slices ]

        self._major_slice = self._slices[0]
        if rank > 1:
            self._minor_slices = self._slices[1:]
        else:
            self._minor_slices = None

        self._init_iter()

        logger.debug('end Hdf5Data.__init__()')

    def _init_iter(self):
        '''Initialize the iterator'''
        rank = len(self.var.shape)
        if rank > 1 or None in self.var.maxshape:
            self.iter = islice(iter(self.var), self._major_slice.start, self._major_slice.stop, self._major_slice.step)
        else:
            self.iter = imap(lambda x: x[self._major_slice.start:self._major_slice.stop:self._major_slice.step], [self.var])

        
    def __getitem__(self, slices):
        # for a 1d slice, there will (should) only be one slice
        if type(slices) in (slice, ss):
            slices = (slices,)

        # convert all regular slices into stackable slices for the addition
        converted_slices = []
        for s in slices:
            if type(s) == ss:
                converted_slices.append(s)
            elif type(s) in (slice, int):
                converted_slices.append(ss(s))
            else:
                raise TypeError("__getitem__ should be called with a list of slices (or StackableSlices), not {}".format( [type(s) for s in slices ]))
        slices = converted_slices

        if len(slices) != len(self.shape):
            raise ValueError("dataset has {0} dimensions, but the slice has {1} dimensions".format(len(slices), len(self.shape)))

        subset_slices = [ orig_slice + subset_slice for orig_slice, subset_slice in zip(self._slices, slices) ]

        return Hdf5Data(self.var, subset_slices)

    def __iter__(self):
        logger.debug('returning from __iter__')
        return self

    def next(self):
        try:
            x = self.iter.next()
            if self._minor_slices:
                # Can't actually index with sequence of stackable slices... convert to slices
                minor_slices = [ s.slice for s in self._minor_slices ]
                return x[ minor_slices ]
            else:
                return x
        except StopIteration:
            self._init_iter()
            raise

    def __len__(self): return self.var.shape[0]

    @property
    def dtype(self):
        return self.var.dtype

    @property
    def shape(self):
        logger.debug("in shape with major_slice=%s and slices=%s", self._major_slice, self._slices)
        myshape = self.var.shape
        true_slices = [ s.slice for s in self._slices ]
        myshape = sliced_shape(true_slices, myshape)
        logger.debug("leaving shape with result %s", myshape)
        return myshape

    def byteswap(self):
        x = self.var.__getitem__(self._slices)
        return x.byteswap()

def sliced_shape(slice_, shape_):
    assert len(slice_) == len(shape_)
    rv = [ sh if sl == slice(None) else len(range(sh)[sl]) for sl, sh in zip(slice_, shape_) ]
    return tuple(rv)

if __name__ == "__main__":
    import sys
    from werkzeug.serving import run_simple

    application = HDF5Handler(sys.argv[1])
    run_simple('localhost', 8002, application, use_reloader=True)
