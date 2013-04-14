from setuptools import setup, find_packages
import sys, os

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
NEWS = open(os.path.join(here, 'NEWS.txt')).read()


version = '0.2'

install_requires = [
    # List your project dependencies here.
    # For more details, see:
    # http://packages.python.org/distribute/setuptools.html#declaring-dependencies
    'h5py',
]


setup(name='pydap.handlers.hdf5',
    version=version,
    description="HDF5 handler for Pydap",
    long_description=README + '\n\n' + NEWS,
    classifiers=[
      # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    ],
    keywords='HDF5 opendap dods dap science meteorology oceanography',
    author='Roberto De Almeida',
    author_email='roberto@dealmeida.net',
    url='http://pydap.org/handlers.html#hdf5',
    license='MIT',
    packages=find_packages('src'),
    package_dir = {'': 'src'},
    namespace_packages = ['pydap', 'pydap.handlers'],
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    entry_points="""
        [pydap.handler]
        hdf5 = pydap.handlers.hdf5:HDF5Handler
    """,
)
