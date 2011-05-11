#!/usr/bin/python -w
import random;

size = 100000;
min = -size/1000;
max = size/1000;
def gen():
  
  file = open('yfs1/sort_data','w')
  for i in range(size):
    k = random.randint(min,max);
    file.write(str(k)+"\n")
  file.close()

gen();
