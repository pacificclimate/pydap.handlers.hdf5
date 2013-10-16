from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand
import sys, os

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.rst')).read()
NEWS = open(os.path.join(here, 'NEWS.txt')).read()

class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ['-v', '--tb=no']
        self.test_suite = True
    def run_tests(self):
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)

version = '0.3'

install_requires = [
    'h5py',
    'pupynere >=1.1.2a2',
    'pydap >=3.2.1'
]

sw_path = 'hg+ssh://medusa.pcic.uvic.ca//home/data/projects/comp_support/software'

setup(name='pydap.handlers.hdf5',
    version=version,
    description="HDF5 handler for Pydap",
    long_description=README + '\n\n' + NEWS,
    classifiers=[
      # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    ],
    keywords='hdf5 opendap dods dap science meteorology oceanography',
    author='James Hiebert',
    author_email='james@hiebert.name',
    url='http://pydap.org/handlers.html#hdf5',
    dependency_links = ['{0}/Pydap-3.2@3.2.1#egg=Pydap-3.2.1'.format(sw_path),
                        '{0}/pupynere@912821570233#egg=pupynere-1.1.2a2'.format(sw_path)],
    license='MIT',
    packages=find_packages('src'),
    package_dir = {'': 'src'},
    namespace_packages = ['pydap', 'pydap.handlers'],
    package_data={'': ['pydap/handlers/hdf5/data/*.h5']},
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    entry_points="""
        [pydap.handler]
        hdf5 = pydap.handlers.hdf5:HDF5Handler
        nc = pydap.handlers.hdf5:HDF5Handler
    """,
    tests_require=['pytest', 'numpy'],
    cmdclass = {'test': PyTest},
)
