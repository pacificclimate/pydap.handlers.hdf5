import os
import re
import time
from stat import ST_MTIME
from email.utils import formatdate

import numpy as np

import h5py

from pydap.model import *
from pydap.handlers.lib import BaseHandler
from pydap.exceptions import OpenFileError


class HDF5Handler(BaseHandler):

    extensions = re.compile(r"^.*\.(h5|hdf5)$", re.IGNORECASE)

    def __init__(self, filepath):
        BaseHandler.__init__(self)

        try:
            self.fp = h5py.File(filepath, 'r')
        except Exception, exc:
            message = 'Unable to open file %s: %s' % (filepath, exc)
            raise OpenFileError(message)

        self.additional_headers.append(
            ('Last-modified', (formatdate(time.mktime(time.localtime( os.stat(
                filepath)[ST_MTIME]))))))

        # build dataset
        name = os.path.split(filepath)[1]
        self.dataset = DatasetType(name, attributes={
            "NC_GLOBAL": dict(self.fp.attrs),
        })

        if self.fp.attrs.get('Map Projection') == 'Equidistant Cylindrical':
            lat_bnds = np.linspace(
                self.fp.attrs['Northernmost Latitude'],
                self.fp.attrs['Southernmost Latitude'],
                self.fp.attrs['Number of Lines']+1)
            lat = (lat_bnds[:-1] + lat_bnds[1:])/2.
            lon_bnds = np.linspace(
                self.fp.attrs['Westernmost Longitude'],
                self.fp.attrs['Easternmost Longitude'],
                self.fp.attrs['Number of Columns']+1)
            lon = (lon_bnds[:-1] + lon_bnds[1:])/2.
            dims = lat, lon
        else:
            dims = None

        for name in self.fp:
            if dims and self.fp[name].shape == (len(lat), len(lon)):
                g = self.dataset[name] = GridType(name, dict(self.fp[name].attrs))
                g[name] = BaseType(name, self.fp[name], ('lat', 'lon'),
                    dict(self.fp[name].attrs))
                g['lat'] = BaseType('lat', dims[0], None,
                    dict(axis='Y', units='degrees_north'))
                g['lon'] = BaseType('lon', dims[1], None,
                    dict(axis='X', units='degrees_east'))
            else:
                self.dataset[name] = BaseType(name, self.fp[name], None,
                    dict(self.fp[name].attrs))

        if dims:
            self.dataset['lat'] = g['lat']
            self.dataset['lon'] = g['lon']


if __name__ == "__main__":
    import sys
    from werkzeug.serving import run_simple

    application = HDF5Handler(sys.argv[1])
    run_simple('localhost', 8001, application, use_reloader=True)
