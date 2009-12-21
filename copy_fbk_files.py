#! /usr/bin/env python
import os, sys
from subprocess import * 
from string import *


test = Popen("ssh desvignes@sds1 \"ls -1 /data1T?/fbk/%s*p03.fbk\""%(sys.argv[1]),shell=True, stdout=PIPE)
files_t = test.communicate()[0]
files = files_t.splitlines()

print files 

for file in files:
  idx = file.rfind('/') +1
  #print file[idx:idx+8]
  #print file[:-6]
  dir = "/data5/BON/%s"%(file[idx:idx+8])
  try:
    os.mkdir(dir)
  except:
    print 'Directory %s already exists'%dir
  os.chdir(dir)
  cmd = "rsync -av --progress desvignes@sds1:%s3.fbk ."%file[:-5]
  os.system(cmd)
  cmd = "rsync -av --progress desvignes@sds1:%s2.fbk ."%file[:-5]
  os.system(cmd)
  #cmd = "rsync -rv --append --progress desvignes@sds1:%s1.fbk ."%file[:-5]
  #os.system(cmd)
  #cmd = "rsync -rv --append --progress desvignes@sds1:%s0.fbk ."%file[:-5]
  #os.system(cmd)
  cmd = "rsync -av --progress desvignes@sds2:%s1.fbk ."%file[:-5]
  os.system(cmd)
  cmd = "rsync -av --progress desvignes@sds2:%s0.fbk ."%file[:-5]
  os.system(cmd)
  #print cmd




