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
from .stack_slice import StackableSlice

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
        logger.debug('Hdf5Data.__init__({}, {})'.format(var, slices))

        rank = len(var.shape)
        assert rank > 0

        if not slices:
            self._slices = [ StackableSlice(None, None, None) for i in range(rank) ]
        else:
            assert len(slices) == rank
            self._slices = [ StackableSlice(s.start, s.stop, s.step) for s in slices ]

        self._major_slice = self._slices[0]
        if rank > 1:
            self._minor_slices = self._slices[1:]
        else:
            self._minor_slices = None

        self._init_iter()

        logger.debug('end Hdf5Data.__init__()')

    def _init_iter(self):
        '''Initialize the iterator'''
        if self._major_slice.start:
            self.pos = self._major_slice.start
        else:
            self.pos = 0

    def __getitem__(self, slices):
        logger.debug('HDF5Data({}.__getitem({})'.format(self.var, slices))
        # There are three types of acceptable keys...
        # A single integer
        if type(slices) == int:
            slices = (StackableSlice(slices, slices+1, 1),)
        # A single slice for a 1d dataset
        elif type(slices) in (slice, StackableSlice):
            assert self.rank == 1
            slices = (slices,)
        # A tuple of slices where the number of elements in the tuple equals the number of dimensions in the dataset
        elif type(slices) in (tuple, list):
            if len(slices) != self.rank:
                raise ValueError("dataset has {0} dimensions, but the slice has {1} dimensions".format(len(slices), self.rank))
        else:
            raise TypeError()

        # convert all regular slices into stackable slices for the addition
        converted_slices = []
        for s in slices:
            if type(s) == StackableSlice:
                converted_slices.append(s)
            elif type(s) == int:
                converted_slices.append(StackableSlice(s, s+1, 1))
            elif type(s) == slice:
                converted_slices.append(StackableSlice(s.start, s.stop, s.step))
            else:
                raise TypeError("__getitem__ should be called with a list of slices (or StackableSlices), not {}".format( [type(s) for s in slices ]))
        slices = converted_slices

        subset_slices = [ orig_slice + subset_slice for orig_slice, subset_slice in zip(self._slices, slices) ]

        return Hdf5Data(self.var, subset_slices)

    def __iter__(self):
        logger.debug('returning from __iter__')
        return Hdf5Data(self.var, self._slices)

    def next(self):
        stop = self._major_slice.stop if self._major_slice.stop else self.var.shape[0]
        step = self._major_slice.step if self._major_slice.step else 1
        if self.pos < stop:

            # Special case: for 1d variables, non-record variables return
            # output on the first iteration in a single numpy array
            if self.rank == 1 and self.var.maxshape != (None,):
                self.pos = float('inf')
                return self.var[self._major_slice.slice]

            x = self.var[self.pos]
            self.pos += step
            if self._minor_slices:
                # Can't actually index with sequence of stackable slices... convert to slices
                minor_slices = [ s.slice for s in self._minor_slices ]
                return x[ minor_slices ]
            else:
                return x
        else:
            self._init_iter()
            raise StopIteration

    def __len__(self): return self.var.shape[0]

    @property
    def dtype(self):
        return self.var.dtype

    @property
    def shape(self):
        logger.debug("HDF5Data({}).shape : major_slice={} and slices={}".format(self.var, self._major_slice, self._slices))
        myshape = self.var.shape
        true_slices = [ s.slice for s in self._slices ]
        myshape = sliced_shape(true_slices, myshape)
        logger.debug("leaving shape with result %s", myshape)
        return myshape

    @property
    def rank(self):
        return len(self.shape)

    def byteswap(self):
        x = self.var.__getitem__(self._slices)
        return x.byteswap()

    def astype(self, type_):
        slices = tuple([ ss.slice for ss in self._slices ])
        x = self.var.__getitem__(slices)
        return x.astype(type_)

def sliced_shape(slice_, shape_):
    assert len(slice_) == len(shape_)
    rv = [ sh if sl == slice(None) else len(range(sh)[sl]) for sl, sh in zip(slice_, shape_) ]
    return tuple(rv)

if __name__ == "__main__":
    import sys
    from werkzeug.serving import run_simple

    application = HDF5Handler(sys.argv[1])
    run_simple('localhost', 8002, application, use_reloader=True)
