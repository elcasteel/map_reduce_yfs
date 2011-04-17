// lock client interface.

#ifndef extent_client_cache_h

#define extent_client_cache_h  

#include <string>
#include "lock_client_cache.h"
#include <map>
#include "tprintf.h"
#include "extent_protocol.h"
#include <pthread.h>
#include "rpc/slock.h"
#include "extent_client.h"

enum extent_state{NORMAL,REMOVED,DIRTY,BAD};

struct extent_cache_t{

   extent_protocol::extentid_t id;
   std::string buf;
   extent_protocol::attr attr;
   extent_state state;
   int ret;
};

class extent_client_cache: public extent_client{

protected:

  pthread_mutex_t mu;
  map<extent_protocol::extentid_t,extent_cache_t> ext_map;

 public:
  extent_client_cache(std::string dst):extent_client(dst){
    pthread_mutex_init(&mu,NULL);
  };

  extent_protocol::status get(extent_protocol::extentid_t eid,
                              std::string &buf){
    ScopedLock sl(&mu);
    tprintf("\nMaking a get call on the cache\n");
    int ret = extent_protocol::OK;
    ret = fillin(eid);
    if(ret != extent_protocol::OK){
      tprintf("\nfailed to fillin in get call for extent cache\n");
      return ret;
    }
    if(ext_map[eid].state==REMOVED) {
       tprintf("\nfile is being removed so does not exist\n");
       return extent_protocol::NOENT;
    }
    buf = ext_map[eid].buf;
    ext_map[eid].attr.atime = time(NULL);
    return ret;
    
};


  extent_protocol::status getattr(extent_protocol::extentid_t eid,
                                  extent_protocol::attr &a){
    ScopedLock sl(&mu);
    tprintf("\nMaking a getattr call on the cache\n");
    int ret = extent_protocol::OK;
    ret = fillin(eid);
    if(ret != extent_protocol::OK){           
      tprintf("\nfailed to fillin in get call for extent cache\n");
      return ret;
    }
    if(ext_map[eid].state==REMOVED) {
       tprintf("\nfile is being removed so does not exist\n");
       return extent_protocol::NOENT;
    }
    extent_cache_t tmp = ext_map[eid];
    a.size = tmp.attr.size;
    a.atime = tmp.attr.atime;
    a.mtime = tmp.attr.mtime;
    a.ctime = tmp.attr.ctime;
    return ret;

};


  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf){
    ScopedLock sl(&mu);
    tprintf("\nMaking a put call on the cache\n");

    int ret = extent_protocol::OK;
    fillin(eid);
//    ret = fillin(eid);            
//    if(ret != extent_protocol::OK){
//      tprintf("\nfailed to fillin in get call for extent cache\n");
//      return ret;
//    }

    ext_map[eid].state = DIRTY;
    extent_cache_t tmp = ext_map[eid];
    tmp.buf = buf;
    tmp.attr.size = tmp.buf.length();
    tmp.attr.mtime = time(NULL);
    tmp.attr.ctime = time(NULL);
    ext_map[eid] = tmp;
    return ret;
};


  extent_protocol::status remove(extent_protocol::extentid_t eid){
    ScopedLock sl(&mu);
    tprintf("\nMaking a remove call on the cache\n");

    int ret = extent_protocol::OK;
    ret = fillin(eid);
    if(ret != extent_protocol::OK){
      tprintf("\nfailed to fillin in get call for extent cache\n");
      return ret;
    }
    ext_map[eid].state = REMOVED;
};


  extent_protocol::status get_remote(extent_protocol::extentid_t eid,
                              std::string &buf){
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::get, eid, buf);
  return ret;

};
  extent_protocol::status getattr_remote(extent_protocol::extentid_t eid,
                                  extent_protocol::attr &a){
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::getattr, eid, a);
  return ret;
};
  extent_protocol::status put_remote(extent_protocol::extentid_t eid, std::string buf){
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  ret = cl->call(extent_protocol::put, eid, buf, r);
  return ret;
};
  extent_protocol::status remove_remote(extent_protocol::extentid_t eid){
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  ret = cl->call(extent_protocol::remove, eid, r);
  return ret;
};
  extent_protocol::status flush(extent_protocol::extentid_t eid){
    ScopedLock sl(&mu);
    tprintf("\nflushing %llu!\n",eid);
    //assert it exists in table...
    int ret = extent_protocol::OK;

    if(ext_map[eid].state==DIRTY){
	ret = put_remote(eid,ext_map[eid].buf);
    }else if(ext_map[eid].state==REMOVED){
	ret = remove_remote(eid);

    }
    ext_map.erase(eid);

    return ret;
};
//fills in the map entry with data from the server, doesnt lock
  extent_protocol::status fillin(extent_protocol::extentid_t eid){
    tprintf("\nFilling in data\n");   
    //assert that table entry is empty
    int ret = extent_protocol::OK;
    if(ext_map.find(eid)!=ext_map.end()){
      if(ext_map[eid].state==BAD){
	tprintf("\nalready got bad return\n");
        return ext_map[eid].ret;
      } 
      tprintf("\nthe call is local\n");
      return ret;
   }
 
    extent_cache_t temp;
    temp.state = NORMAL;
    ret = get_remote(eid,temp.buf);
    if(ret!=extent_protocol::OK){
      tprintf("\nfailed to get string in fillin\n");
      temp.state=BAD;
      temp.ret = ret;
    }
    ret = getattr_remote(eid,temp.attr);
    if(ret!=extent_protocol::OK){                  
      tprintf("\nfailed to get string in fillin\n");
      temp.state=BAD;
      temp.ret = ret;
    }
    tprintf("\nfilled in entry from server\n");
    ext_map[eid] = temp;
    return ret;

};


};


class extent_release: public lock_release_user {
 private:
  extent_client_cache *ec;

 public:
  extent_release(extent_client_cache *e){ ec = e; };
  void dorelease(lock_protocol::lockid_t lid) { ec->flush(lid);  };
  
};



#endif
