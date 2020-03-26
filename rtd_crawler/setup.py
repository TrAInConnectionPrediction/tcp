from distutils.core import setup
from Cython.Build import cythonize
import numpy

setup(name='parser speedups',
      ext_modules=cythonize("speed.pyx", annotate=True, language_level=3),
      include_dirs=[numpy.get_include()])