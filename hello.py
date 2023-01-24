import IPython.nbformat.current as nbf
import Ipython
nb = nbf.read(open('test.py', 'r'), 'py')
nbf.write(nb, open('test.ipynb', 'w'), 'ipynb')