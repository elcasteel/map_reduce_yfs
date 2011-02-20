// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>



yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);

}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:

  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  return r;
}

int
yfs_client::create(inum parent, const char* name, inum &ino)
{
  int ret = IOERR;
  printf("\nCreating file in yfs_client.");
  //generate ino with rand and set file bit to 1
  ino = rand();
  ino = ino | 0x80000000;
  
  //add name/ino to parent dir
  string b;
  if(ec->get(ino,b)==extent_protocol::OK){
    printf("\n had an inum collision!");
    return NOENT;
  }

  if(ec->get(parent,b)!=extent_protocol::OK){
    printf("\n no parent dir found");
    return NOENT;
  }

  cout << "\n dir contents are "<<b;  
  
  b.append(",");
  ostringstream ost;
  ost << ino;
  b.append(ost.str());
  b.append(",");
  b.append(name);
  b.append(",");  

  cout << "\n dir contents are now "<<b;

  if((ret = ec->put(parent,b)) != extent_protocol::OK){
    printf("\n failed to write to the directory");
    return ret;
  }

  //add ino to extent map

  if((ret=ec->put(ino,""))!=extent_protocol::OK){
    printf("\n failed to create file in the extent server");
    return ret;
  }

    

  return OK;
}  

int
yfs_client::lookup(inum parent, const char* name, inum &ino)
{
  
  printf("\nLooking up file %s in parent id %llu",name,parent);
  //read directory listing in parent
  vector<dirent> dir_contents;
  if(readdir(parent,dir_contents)!=OK){
    printf("\n failed to read dir contents in lookup.");
    return NOENT;
  }

  
  string temp_name = string(name);

  //find inum associated with name in directory contents
  vector<dirent>::iterator it;
  for(it=dir_contents.begin();it!=dir_contents.end();++it){
    //TODO ensure string compare works
    dirent temp = *it;
    if(temp.name==temp_name){
       printf("\n found the file");
       ino = temp.inum;
       return OK;
    }

  }  

  printf("\n no file found");
  return NOENT;
}  


int                 
yfs_client::readdir(inum dir,vector<dirent>& dir_vector)
{
  printf("\nReading directory in yfs_client for inum %llu",dir);
  string str;
  if(ec->get(dir,str)!=extent_protocol::OK)
  {
    printf("\n failed to get directory contents from extent client");
    return NOENT;
  }

  if(isfile(dir)){
    printf("\n inum refers to a file, not a directory");
    return NOENT;
  }
  

  //parse contents of dir into dir_vector
  if(str.length()==0){
    printf("\n directory is empty");
    return OK;
  }

  //parse contents
  char *c, buf[str.length()];
  strcpy(buf,str.c_str());

  printf("\n directory contents are: %s",buf);


  c = strtok(buf,",");
  dirent entry;

  while(c!=NULL){
     std::istringstream ist(c);
     inum finum;
     ist>>finum;
     
     c = strtok(NULL,",");
     entry.name = c;
     entry.inum = finum;
     dir_vector.push_back(entry);  
     printf("\n File:%s mapped to %llu",entry.name.c_str(),entry.inum);
     c = strtok(NULL,",");
  }
  printf("\nDirectory read.");
  return OK;
}

int
yfs_client::get(inum inum, string& str)
{
  printf("\nGetting id %llu from extent server",inum);
  if(ec->get(inum,str)!=extent_protocol::OK){
     printf("\n failed to get from extent server");
     return NOENT;
  }
  printf("\n Got %s from server",str.c_str());
  return OK;
}    

int
yfs_client::put(inum inum, string str)
{
  printf("\nPutting id %llu to extent server",inum);
  if(ec->put(inum,str)!=extent_protocol::OK){
     printf("\n failed to put to extent server");
     return NOENT;
  }
  printf("\n Put %s to server",str.c_str());
  return OK;
}    

