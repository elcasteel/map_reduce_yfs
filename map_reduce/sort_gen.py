#!/usr/bin/python -w
import random;

size = 100;
min = -size/10;
max = size/10;
def gen():
  
  file = open('yfs1/sort_data','w')
  for i in range(size):
    k = random.randint(min,max);
    file.write(str(k)+"\n")
  file.close()

gen();
