import os
import re
import time
from stat import ST_MTIME
from email.utils import formatdate

import numpy as np

from pupynere import netcdf_file

from pydap.model import *
from pydap.handlers.lib import BaseHandler
from pydap.exceptions import OpenFileError


class NetCDFHandler(BaseHandler):

    extensions = re.compile(r"^.*\.(nc|cdf)$", re.IGNORECASE)

    def __init__(self, filepath):
        BaseHandler.__init__(self)

        try:
            self.fp = netcdf_file(filepath)
        except Exception, exc:
            message = 'Unable to open file %s: %s' % (filepath, exc)
            raise OpenFileError(message)

        self.additional_headers.append(
                ('Last-modified', (formatdate(time.mktime(time.localtime(os.stat(filepath)[ST_MTIME]))))))

        # shortcuts
        vars = self.fp.variables
        dims = self.fp.dimensions

        # build dataset
        name = os.path.split(filepath)[1]
        self.dataset = DatasetType(name, attributes=dict(NC_GLOBAL=self.fp._attributes))
        for dim in dims:
            if dims[dim] is None:
                self.dataset.attributes['DODS_EXTRA'] = {'Unlimited_Dimension': dim}
                break

        # add grids
        grids = [var for var in vars if var not in dims]
        for grid in grids:
            self.dataset[grid] = GridType(grid, vars[grid]._attributes)
            # add array
            self.dataset[grid][grid] = BaseType(grid, NetcdfData(vars[grid]),
                    vars[grid].dimensions, vars[grid]._attributes)
            # add maps
            for dim in vars[grid].dimensions:
                self.dataset[grid][dim] = BaseType(dim, vars[dim][:],
                        None, vars[dim]._attributes)

        # add dims
        for dim in dims:
            self.dataset[dim] = BaseType(dim, vars[dim][:],
                    None, vars[dim]._attributes)

    def close(self):
        self.fp.close()


class NetcdfData(object):
    """
    A wrapper for Netcdf variables, making them behave more like Numpy arrays.

    """
    def __init__(self, var):
        self.var = var

    @property
    def dtype(self):
        return np.dtype(self.var.typecode())

    @property
    def shape(self):
        return self.var.shape

    # Comparisons are passed to the data.
    def __eq__(self, other): return self.var[:] == other
    def __ne__(self, other): return self.var[:] != other
    def __ge__(self, other): return self.var[:] >= other
    def __le__(self, other): return self.var[:] <= other
    def __gt__(self, other): return self.var[:] > other
    def __lt__(self, other): return self.var[:] < other

    # Implement the sequence and iter protocols.
    def __getitem__(self, index): return self.var[index]
    def __len__(self): return self.shape[0]
    def __iter__(self): return iter(self.var[:])    


if __name__ == "__main__":
    import sys
    from werkzeug.serving import run_simple

    application = NetCDFHandler(sys.argv[1])
    run_simple('localhost', 8001, application, use_reloader=True)
