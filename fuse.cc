/*
 * receive request from fuse and call methods of yfs_client
 *
 * started life as low-level example in the fuse distribution.  we
 * have to use low-level interface in order to get i-numbers.  the
 * high-level interface only gives us complete paths.
 */

#include <fuse_lowlevel.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "yfs_client.h"

using namespace std;

int myid;
yfs_client *yfs;

int id() { 
  return myid;
}

yfs_client::status
getattr(yfs_client::inum inum, struct stat &st)
{
  yfs_client::status ret;

  bzero(&st, sizeof(st));

  st.st_ino = inum;
  printf("getattr %016llx %d\n", inum, yfs->isfile(inum));
  if(yfs->isfile(inum)){
     yfs_client::fileinfo info;
     ret = yfs->getfile(inum, info);
     if(ret != yfs_client::OK)
       return ret;
     st.st_mode = S_IFREG | 0666;
     st.st_nlink = 1;
     st.st_atime = info.atime;
     st.st_mtime = info.mtime;
     st.st_ctime = info.ctime;
     st.st_size = info.size;
     printf("   getattr -> size: %llu\n", info.size);
   } else {
     yfs_client::dirinfo info;
     ret = yfs->getdir(inum, info);
     if(ret != yfs_client::OK)
       return ret;
     st.st_mode = S_IFDIR | 0777;
     st.st_nlink = 2;
     st.st_atime = info.atime;
     st.st_mtime = info.mtime;
     st.st_ctime = info.ctime;
     printf("   getattr -> %lu %lu %lu\n", info.atime, info.mtime, info.ctime);
   }
   return yfs_client::OK;
}


void
fuseserver_getattr(fuse_req_t req, fuse_ino_t ino,
          struct fuse_file_info *fi)
{
    struct stat st;
    yfs_client::inum inum = ino; // req->in.h.nodeid;
    yfs_client::status ret;

    ret = getattr(inum, st);
    if(ret != yfs_client::OK){
      fuse_reply_err(req, ENOENT);
      return;
    }
    fuse_reply_attr(req, &st, 0);
}

void
fuseserver_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi)
{
  printf("fuseserver_setattr 0x%x\n", to_set);
  if (FUSE_SET_ATTR_SIZE & to_set) {
    printf("   fuseserver_setattr set size to %zu\n", attr->st_size);
    struct stat st;
    // You fill this in for Lab 2
    if(getattr(ino,st)!=yfs_client::OK){
       printf("\n failed to getattr in setattr");
       fuse_reply_err(req, ENOSYS);
       return;   
    }

    string buf;
    if(yfs->get(ino,buf)!=yfs_client::OK){
       printf("\n failed to get buffer from yfs client.");
       fuse_reply_err(req, ENOSYS);
       return;
    }

    if(st.st_size != buf.length()){
      printf("\n size attribute was off in the yfs_client (setattr)");
     }
    
    if(st.st_size>(attr->st_size))
    {
      //need to truncate
      int trunc = st.st_size-(attr->st_size);
      printf("\n truncating file size by %d",trunc);
      buf = buf.substr(0,attr->st_size);

    }else if(st.st_size<(attr->st_size)){
      //need to pad
      int padNum = (attr->st_size)-st.st_size;
      printf("\n padding with %d bytes",padNum);
      int i;
      for(i=0;i<padNum;i++)
      {
         buf.append(" ");
      }

    }
    //else we need do nothing
    if(yfs->put(ino,buf)!=yfs_client::OK){
       printf("\n unable to put string back in yfs");
       fuse_reply_err(req, ENOSYS);
       return;
    }
   //get attr back from the source
   if(getattr(ino,st)!=yfs_client::OK){
       printf("\n failed to getattr in setattr (2)");
       fuse_reply_err(req, ENOSYS);
       return;
    }


    fuse_reply_attr(req, &st, 0);


  } else {
    fuse_reply_err(req, ENOSYS);
  }
}

void
fuseserver_read(fuse_req_t req, fuse_ino_t ino, size_t size,
      off_t off, struct fuse_file_info *fi)
{
  printf("\nReading file with inode %lu",ino);
  //printf("\noffset is %u and size is %u",off,size);
  cout << "\noffset is "<<off<<" and size is "<<size;
  // You fill this in for Lab 2
  string buf;

  if(yfs->get(ino,buf)!=yfs_client::OK){
    printf("\n failed to get from yfs client in fuse read");
    fuse_reply_err(req, ENOSYS);
    return;
  }
  printf("\n yfs client returned string %s",buf.c_str());  
  //if(size==0) size = buf.length();
  //const char *b = buf.substr(off,size).c_str();
  char * b = (char *) malloc(size);
  memcpy(b,buf.data()+off,size);

  fuse_reply_buf(req, b, size);
  printf("\nRead file, returned: %s",b);
  free(b);
  return;

}

void
fuseserver_write(fuse_req_t req, fuse_ino_t ino,
  const char *buf, size_t size, off_t off,
  struct fuse_file_info *fi)
{
  printf("\nWriting to file %lu",ino);
  // You fill this in for Lab 2
  string s;
  if(yfs->get(ino,s)!=yfs_client::OK){
   printf("\n failed to get string from yfs client");
   fuse_reply_err(req, ENOSYS);
   return;
  }
  //cout <<"\nGiven size is "<<size<< " and buf size is "<< strlen(buf);
  
  string end;
  int length = s.length();
  //writing past length
  if(off>length){
    s.append((size_t)(off-length),'\0'); 
    s.append(buf,size);

  }else{
    //if theres leftover string
    if(off + size < length)
       end = s.substr(off+size,length);
    s = s.substr(0,off);
    s = s.append(buf,size);
    if(off+size < length)
        s.append(end);
    }
  printf("\n writing size %d string %s to file",size,s.c_str());

  if(yfs->put(ino,s)!=yfs_client::OK){
     printf("\n failed to write to yfs client");
     fuse_reply_err(req, ENOSYS);
     return;
  }
  

  fuse_reply_write(req, size);

}

yfs_client::status
fuseserver_createhelper(fuse_ino_t parent, const char *name,
     mode_t mode, struct fuse_entry_param *e)
{
  printf("\nCreating file in fuse with name %s under parent %lu",name,parent);
  
  // In yfs, timeouts are always set to 0.0, and generations are always set to 0
  e->attr_timeout = 0.0;
  e->entry_timeout = 0.0;
  e->generation = 0;
  // You fill this in for Lab 2
  //fill in e->ino (inode) (unsigned long)
  yfs_client::inum ino;
  yfs_client::inum yfs_parent = parent;

  int ret = yfs->create(yfs_parent,name,ino);
  if(ret!=yfs_client::OK){
    printf("\nfuse failed to create file in create helper."); 
    return ret;
  }
  

  //fill in e->attr which is a stat struct with getattr call
  struct stat st;
  ret = getattr(ino,st);
  if(ret!=yfs_client::OK){
    printf("\nfuse failed to getattr in create helper.");
    return ret;
  }
  //fill in data
  e->ino = ino;
  e->attr = st;
  
  printf("\nFile created with inode %llu",ino);
  return yfs_client::OK;
}

void
fuseserver_create(fuse_req_t req, fuse_ino_t parent, const char *name,
   mode_t mode, struct fuse_file_info *fi)
{
  struct fuse_entry_param e;
  yfs_client::status ret;
  if( (ret = fuseserver_createhelper( parent, name, mode, &e )) == yfs_client::OK ) {
    fuse_reply_create(req, &e, fi);
  } else {
		if (ret == yfs_client::EXIST) {
			fuse_reply_err(req, EEXIST);
		}else{
			fuse_reply_err(req, ENOENT);
		}
  }
}

void fuseserver_mknod( fuse_req_t req, fuse_ino_t parent, 
    const char *name, mode_t mode, dev_t rdev ) {
  struct fuse_entry_param e;
  yfs_client::status ret;
  if( (ret = fuseserver_createhelper( parent, name, mode, &e )) == yfs_client::OK ) {
    fuse_reply_entry(req, &e);
  } else {
		if (ret == yfs_client::EXIST) {
			fuse_reply_err(req, EEXIST);
		}else{
			fuse_reply_err(req, ENOENT);
		}
  }
}

void
fuseserver_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  struct fuse_entry_param e;
  // In yfs, timeouts are always set to 0.0, and generations are always set to 0
  e.attr_timeout = 0.0;
  e.entry_timeout = 0.0;
  e.generation = 0;
  bool found = false;

  // You fill this in for Lab 2
  // Look up the file named `name' in the directory referred to by
  // `parent' in YFS. If the file was found, initialize e.ino and
  // e.attr appropriately.
  
  yfs_client::inum ino;
  if(yfs->lookup(parent,name,ino)!=yfs_client::OK){
     printf("\nLookup failed in fuse for name %s and parent id %lu",name,parent);
  }else{
     printf("\nLookup in fuse found id %llu",ino);
     found = true;
     e.ino = ino;
     struct stat st;
     if(getattr(ino,st)!=yfs_client::OK){
         printf("\nfailed to getattr in fuse lookup");
         found = false;
     }else{
         printf("\nLookup in fuse succeeded, found attributes.");
         e.attr = st;
     }
  }

  if (found)
    fuse_reply_entry(req, &e);
  else
    fuse_reply_err(req, ENOENT);
}


struct dirbuf {
    char *p;
    size_t size;
};

void dirbuf_add(struct dirbuf *b, const char *name, fuse_ino_t ino)
{
    struct stat stbuf;
    size_t oldsize = b->size;
    b->size += fuse_dirent_size(strlen(name));
    b->p = (char *) realloc(b->p, b->size);
    memset(&stbuf, 0, sizeof(stbuf));
    stbuf.st_ino = ino;
    fuse_add_dirent(b->p + oldsize, name, &stbuf, b->size);
}

#define min(x, y) ((x) < (y) ? (x) : (y))

int reply_buf_limited(fuse_req_t req, const char *buf, size_t bufsize,
          off_t off, size_t maxsize)
{
  if (off < bufsize)
    return fuse_reply_buf(req, buf + off, min(bufsize - off, maxsize));
  else
    return fuse_reply_buf(req, NULL, 0);
}

void
fuseserver_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
          off_t off, struct fuse_file_info *fi)
{
  yfs_client::inum inum = ino; // req->in.h.nodeid;
  struct dirbuf b;

  printf("fuseserver_readdir\n");

  if(!yfs->isdir(inum)){
    fuse_reply_err(req, ENOTDIR);
    return;
  }

  memset(&b, 0, sizeof(b));


  // You fill this in for Lab 2
  // Ask the yfs_client for the file names / i-numbers
  // in directory inum, and call dirbuf_add() for each.
  vector<yfs_client::dirent> dir_vector;
  if(yfs->readdir(inum,dir_vector)==yfs_client::OK){
     vector<yfs_client::dirent>::iterator it;
     for(it = dir_vector.begin();it!=dir_vector.end();++it){

        yfs_client::dirent tmp = *it;
        dirbuf_add(&b,tmp.name.c_str(),tmp.inum);

     }

  }

  reply_buf_limited(req, b.p, b.size, off, size);
  free(b.p);
}


void
fuseserver_open(fuse_req_t req, fuse_ino_t ino,
     struct fuse_file_info *fi)
{
  fuse_reply_open(req, fi);
}

void
fuseserver_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
     mode_t mode)
{
  struct fuse_entry_param e;
  // In yfs, timeouts are always set to 0.0, and generations are always set to 0
  e.attr_timeout = 0.0;
  e.entry_timeout = 0.0;
  e.generation = 0;

  // You fill this in for Lab 3
#if 0
  fuse_reply_entry(req, &e);
#else
  fuse_reply_err(req, ENOSYS);
#endif
}

void
fuseserver_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{

  // You fill this in for Lab 3
  // Success:	fuse_reply_err(req, 0);
  // Not found:	fuse_reply_err(req, ENOENT);
  fuse_reply_err(req, ENOSYS);
}

void
fuseserver_statfs(fuse_req_t req)
{
  struct statvfs buf;

  printf("statfs\n");

  memset(&buf, 0, sizeof(buf));

  buf.f_namemax = 255;
  buf.f_bsize = 512;

  fuse_reply_statfs(req, &buf);
}

struct fuse_lowlevel_ops fuseserver_oper;

int
main(int argc, char *argv[])
{
  char *mountpoint = 0;
  int err = -1;
  int fd;

  setvbuf(stdout, NULL, _IONBF, 0);

  if(argc != 4){
    fprintf(stderr, "Usage: yfs_client <mountpoint> <port-extent-server> <port-lock-server>\n");
    exit(1);
  }
  mountpoint = argv[1];

  srandom(getpid());

  myid = random();

  yfs = new yfs_client(argv[2], argv[3]);

  fuseserver_oper.getattr    = fuseserver_getattr;
  fuseserver_oper.statfs     = fuseserver_statfs;
  fuseserver_oper.readdir    = fuseserver_readdir;
  fuseserver_oper.lookup     = fuseserver_lookup;
  fuseserver_oper.create     = fuseserver_create;
  fuseserver_oper.mknod      = fuseserver_mknod;
  fuseserver_oper.open       = fuseserver_open;
  fuseserver_oper.read       = fuseserver_read;
  fuseserver_oper.write      = fuseserver_write;
  fuseserver_oper.setattr    = fuseserver_setattr;
  fuseserver_oper.unlink     = fuseserver_unlink;
  fuseserver_oper.mkdir      = fuseserver_mkdir;

  const char *fuse_argv[20];
  int fuse_argc = 0;
  fuse_argv[fuse_argc++] = argv[0];
#ifdef __APPLE__
  fuse_argv[fuse_argc++] = "-o";
  fuse_argv[fuse_argc++] = "nolocalcaches"; // no dir entry caching
  fuse_argv[fuse_argc++] = "-o";
  fuse_argv[fuse_argc++] = "daemon_timeout=86400";
#endif

  // everyone can play, why not?
  //fuse_argv[fuse_argc++] = "-o";
  //fuse_argv[fuse_argc++] = "allow_other";

  fuse_argv[fuse_argc++] = mountpoint;
  fuse_argv[fuse_argc++] = "-d";

  fuse_args args = FUSE_ARGS_INIT( fuse_argc, (char **) fuse_argv );
  int foreground;
  int res = fuse_parse_cmdline( &args, &mountpoint, 0 /*multithreaded*/, 
        &foreground );
  if( res == -1 ) {
    fprintf(stderr, "fuse_parse_cmdline failed\n");
    return 0;
  }
  
  args.allocated = 0;

  fd = fuse_mount(mountpoint, &args);
  if(fd == -1){
    fprintf(stderr, "fuse_mount failed\n");
    exit(1);
  }

  struct fuse_session *se;

  se = fuse_lowlevel_new(&args, &fuseserver_oper, sizeof(fuseserver_oper),
       NULL);
  if(se == 0){
    fprintf(stderr, "fuse_lowlevel_new failed\n");
    exit(1);
  }

  struct fuse_chan *ch = fuse_kern_chan_new(fd);
  if (ch == NULL) {
    fprintf(stderr, "fuse_kern_chan_new failed\n");
    exit(1);
  }

  fuse_session_add_chan(se, ch);
  // err = fuse_session_loop_mt(se);   // FK: wheelfs does this; why?
  err = fuse_session_loop(se);
    
  fuse_session_destroy(se);
  close(fd);
  fuse_unmount(mountpoint);

  return err ? 1 : 0;
}
