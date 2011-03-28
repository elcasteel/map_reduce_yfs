#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client_cache.h"
#include <vector>
#include "lock_client.h"

#include "lock_protocol.h"
#include "lock_client.h"
#include "lang/verify.h"

using namespace std;


class yfs_client {

 public:
  extent_client_cache *ec;
  lock_client *lc;

 


  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    string name;
    yfs_client::inum inum;
  };

  struct yfs_lock {
    lock_client *_lc;
    inum _id;
    yfs_lock(lock_client *lc,inum id){
      _lc = lc;
      _id = id;
      VERIFY(_lc->acquire(_id)==lock_protocol::OK);
    }
    ~yfs_lock(){
      VERIFY(_lc->release(_id)==lock_protocol::OK);
    }

  };


 private:
  static std::string filename(inum);
  static inum n2i(std::string);
  
  int removeFromDir(inum,inum);

 public:

  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int create(inum, const char*, inum,bool);
  int lookup(inum, const char*, inum&);
  int readdir(inum,vector<dirent>&);
  int put(inum,string);
  int get(inum,string&);
  int remove(inum,inum);
  //expose locking, yfs_client methods are not thread safe
  int lock(inum);
  int release(inum);
};

#endif 
