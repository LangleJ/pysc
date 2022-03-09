from glob import glob
import os
path = 'G:\\Sierra'
match = '*.scid'

files = glob(os.path.join(path, match))
files.sort(key=os.path.getmtime, reverse=True)