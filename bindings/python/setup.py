import os
import sys
import glob

from distutils.core import setup, Extension

currentpath = os.path.dirname(__file__)
rootpath = os.path.join(currentpath, '..', '..')
sources = glob.glob(os.path.join(rootpath, 'engine/*.c'))

#os.environ["CFLAGS"] = "-Wno-strict-prototypes -std=c99 -ggdb -O0"
os.environ["CFLAGS"] = "-Wno-strict-prototypes -std=c99 -O3 -fomit-frame-pointer"

ext_kiwi = Extension('_kiwidb',
	include_dirs=[os.path.join(rootpath, 'engine')],
	sources=sources + [os.path.join('extension', 'kiwi_module.c')],
    libraries=['snappy'])

setup(name='KiwiGraph',
      version='0.1',
      description='Graph database on top of Kiwi KV storage',
      author='Francesco Piccinno',
      author_email='stac.box@gmail.com',
      packages=['kiwi'],
      #py_modules=['indexer'],
      ext_modules=[ext_kiwi])

