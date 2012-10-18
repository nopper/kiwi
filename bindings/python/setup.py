import os
import sys
import glob

from distutils.core import setup, Extension


currentpath = os.path.abspath(os.path.dirname(__file__))
rootpath = os.path.abspath(os.path.join(currentpath, '..', '..'))
sources = glob.glob(os.path.join(rootpath, 'engine/*.c'))

os.environ["CFLAGS"] = "-Wno-strict-prototypes -std=c99 -I%s/engine -ggdb -O0" % rootpath
#os.environ["CFLAGS"] = "-Wno-strict-prototypes -std=c99 -I%s/engine -O3" % rootpath

ext_indexer = Extension('_indexer',
                        include_dirrs=[os.path.join(rootpath, 'engine')],
                        sources=sources + ['indexer_module.c'])

setup(name='indexer',
      version='0.1',
      description='Python binding for indexer',
      py_modules=['indexer'],
      ext_modules=[ext_indexer])

