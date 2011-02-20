// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "rpc/slock.h"

extent_server::extent_server() {
  //init lock (for later)
  pthread_mutex_init(&mu,NULL);
  //insert root directory into map
  extent_t root;
  root.buf="";
  root.id=1;
  root.attr.size=0;
  root.attr.atime = time(NULL);
  root.attr.mtime = time(NULL);
  root.attr.ctime = time(NULL);
  ext_map[root.id] = root;

}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  ScopedLock sl(&mu);
  // You fill this in for Lab 2
  //check if its a new entry
  if(ext_map.find(id)==ext_map.end()){
     extent_t tmp;
     tmp.id = id;
     tmp.buf = buf;
     tmp.attr.size = tmp.buf.length();
     tmp.attr.atime = time(NULL);
     tmp.attr.mtime = time(NULL);
     tmp.attr.ctime = time(NULL);
     ext_map[tmp.id] = tmp;
  }else{
     extent_t tmp = ext_map[id];
     tmp.buf = buf;
     tmp.attr.size = tmp.buf.length();
     tmp.attr.mtime = time(NULL);
     tmp.attr.ctime = time(NULL);
     ext_map[id] = tmp;
  }
  printf("\nPut in id %llu a string of size %d with %s",id,ext_map[id].attr.size,ext_map[id].buf.c_str());
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  ScopedLock sl(&mu);
  // You fill this in for Lab 2.
  //see if it exists
  if(ext_map.find(id)==ext_map.end()){
    printf("\nGet failed to find an entry in the extent server.");
    return extent_protocol::NOENT;
  }
  buf = ext_map[id].buf;
  ext_map[id].attr.atime = time(NULL);
  printf("\nGot from id %llu a string of size %d with %s",id,ext_map[id].attr.size,ext_map[id].buf.c_str());
  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  ScopedLock sl(&mu);
  // You fill this in for Lab 2.
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
  if(ext_map.find(id)==ext_map.end()){
    printf("\nGetattr failed to find an entry in the extent server.");
    return extent_protocol::NOENT;
  }
  
  extent_t tmp = ext_map[id];
  a.size = tmp.attr.size;
  a.atime = tmp.attr.atime;
  a.mtime = tmp.attr.mtime;
  a.ctime = tmp.attr.ctime;
  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  ScopedLock sl(&mu);
  // You fill this in for Lab 2.
  if(ext_map.find(id)==ext_map.end()){
    printf("\nRemove failed to find an entry to remove in the extent server.");
    return extent_protocol::NOENT;
  }
  ext_map.erase(id);
  return extent_protocol::OK;
}

