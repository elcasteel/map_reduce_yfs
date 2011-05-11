#include "node.h"
#include <dirent.h>
#include "../handle.h"


node::node(std::string first,std::string me){
  //init config
  printf("starting node %s %s\n",first.c_str(),me.c_str()); 
  //init mutexes, etc
  
  myid = me;
  primary = first;
  cfg = new config(first,me,this);
  printf("made config \n");
  pthread_mutex_init(&view_mutex,NULL);
  pthread_mutex_init(&map_mutex, NULL);
  
  //register rpc handlers
  rpc_server = cfg ->get_rpcs();
  rpc_server->reg(node::MAP, this ,&node::start_map);
  rpc_server->reg(node::REDUCE, this, &node::start_reduce);
  rpc_server->reg(node::MAP_REDUCE, this ,&node::start_map_reduce);
  rpc_server->reg(node::MAP_DONE,this,&node::mapper_done);
  rpc_server->reg(node::JOINREQ,this,&node::joinreq);
  rpc_server->reg(node::REDUCE_DONE,this,&node::reducer_done);
  tprintf("registering joinreq \n");
  if(first ==me)
  {
     commit_change(1);
  }
  else
  {
    join(first);
  }  
  tprintf("node initialized \n");
}

bool
node::join(std::string m) {
  handle h(m);
  int ret;
  rsm_protocol::joinres r;

  tprintf("rsm::join: %s mylast (%d,%d)\n", m.c_str(), last_myvs.vid, 
          last_myvs.seqno);
 // VERIFY(pthread_mutex_unlock(&rsm_mutex)==0);
  rpcc *cl = h.safebind();
  if (cl != 0) {
    ret = cl->call(node::JOINREQ, cfg->myaddr(), 
		   r, rpcc::to(120000));
  }
 // VERIFY(pthread_mutex_lock(&rsm_mutex)==0);

  if (cl == 0 || ret != rsm_protocol::OK) {
    tprintf("node::join: couldn't reach %s %p %d\n", m.c_str(), 
	   cl, ret);
    return false;
  }
  tprintf("node::join: succeeded %s\n", r.log.c_str());
  cfg->restore(r.log);
  return true;
}
// a node that wants to join an RSM as a server sends a
// joinreq to the RSM's current primary; this is the
// handler for that RPC.
rsm_protocol::status
node::joinreq(std::string m, rsm_protocol::joinres &r)
{
  int ret = rsm_protocol::OK;

  ScopedLock ml(&view_mutex);
//  tprintf("joinreq: src %s last (%d,%d) mylast (%d,%d)\n", m.c_str(), 
//	 last.vid, last.seqno, last_myvs.vid, last_myvs.seqno);
  if (cfg->ismember(m, vid_cur)) {
    tprintf("joinreq: is still a member\n");
    r.log = cfg->dump();
  } else if (cfg->myaddr() != primary) {
    tprintf("joinreq: busy\n");
    ret = rsm_protocol::BUSY;
  } else {
    // We cache vid_commit to avoid adding m to a view which already contains 
    // m due to race condition
    unsigned vid_cache = vid_cur;
    VERIFY (pthread_mutex_unlock(&view_mutex) == 0);
    bool succ = cfg->add(m, vid_cache);
    VERIFY (pthread_mutex_lock(&view_mutex) == 0);
    if (cfg->ismember(m, cfg->vid())) {
      r.log = cfg->dump();
      tprintf("joinreq: ret %d log %s\n:", ret, r.log.c_str());
    } else {
      tprintf("joinreq: failed; proposer couldn't add %d\n", succ);
      ret = rsm_protocol::BUSY;
    }
  }
  return ret;
}


int 
node::start_map_reduce(std::string input_file, std::string output_file, int &a)
{
 master *m;
 unsigned cur_id;
 {
  ScopedLock ml(&map_mutex);
  //make a new master and add it to the map
  last_master_id++;
  tprintf("****NODE****\nmap_reduce starting a job \n"); 

  m = get_master(cfg, last_master_id);
  cur_id= last_master_id;
  m->set_vid(vid_cur);
  master_map[last_master_id]=m;
  //give up the lock
  last_master_id ++;
 }
  
  //start the map reduce
  m->map_reduce(input_file,output_file);
  //return when the job is done
  {
     ScopedLock ma(&map_mutex);
     master_map.erase(cur_id);
     delete m;
  }
  return 1;
}

int
node::start_map(std::string input_file, unsigned job_id, unsigned master_id,std::string output_dir, int &a)
{
  //choose a file name for the output
  tprintf("****NODE**** \nnode starting a map job start_map %s %u\n", input_file.c_str(), job_id);
  stringstream ss(stringstream::in|stringstream::out);
  ss << output_dir.c_str();
  ss << "/";
  ss << job_id;
  ss << "-mapper";
  std::string intermediate_dir = ss.str();
  printf("****NODE**** making output directory for mapper %s \n",intermediate_dir.c_str());
  fflush(stdout);
  int r= mkdir(intermediate_dir.c_str(), 0777);
  VERIFY(r == 0);
  //setup a new thread and mapper object
  pthread_t th;
  mapper* m =get_mapper(input_file, intermediate_dir);
  printf("****NODE**** created new mapper object\n");
  fflush(stdout);
  struct do_map_args* args= new do_map_args();
  args->m= m;
  args->job_id = job_id;
  args->master_id = master_id; 
  args->output_dir = intermediate_dir;
  args->primary = primary;
  printf("****NODE**** creating new thread\n");
  fflush(stdout);
  r = pthread_create(&th,NULL,&do_map,args );
  VERIFY(r == 0); 
  printf("thread created, returning \n");
  return 0;
}
void *
do_map(void* _args)
{
  tprintf("****NODE****\nnode starting a map job do_map (new thread) \n");
  fflush(stdout);
  struct do_map_args* args = (do_map_args*) _args;
  printf("****NODE*** cast arguments\n");
  fflush(stdout);
  args->m->map();
  handle h(args->primary);
  rpcc *cl = h.safebind();
  
  if(cl){
     int r;
     tprintf("****NODE****\nmap job done sending message to master \n");

     int ret = cl->call(node::MAP_DONE,args->master_id, args->output_dir, args->job_id, r);
   } else {
     tprintf("\nmap done call failed!\n");
   }
   delete args->m;
   delete args;
   return 0;
}

int 
node::start_reduce(std::string file_list, std::string job_id,unsigned master_id,std::string output_dir, int &a)
{
    printf("****NODE**** start reduce file list %s job_id %s \n",file_list.c_str(), job_id.c_str());
    //chose a file name for the output
    stringstream outstream(stringstream::in|stringstream::out);
    outstream << output_dir;
    outstream << "/";
    outstream << job_id;
    outstream << ".";
    outstream << master_id;
    outstream << ".out";
    std::string output_file = outstream.str();
       
   //set up a new thread and reducer
   reducer *r = get_reducer();
   struct do_reduce_args* args=new do_reduce_args();
   args->r =r; 
   args->job_id = job_id;
   args->master_id = master_id; 
   args->output_file = output_file;
   args->file_list = file_list;
   args->primary = primary;
   pthread_t th;
   pthread_create(&th,NULL, &do_reduce,args);
   return 0;
}
void *
do_reduce(void *args)
{
   printf("****NODE**** do_reduce starting\n");
   struct do_reduce_args *r_args = (do_reduce_args*) args;
   //call reduce
   r_args->r->start_reduce(r_args->file_list,r_args->output_file);
   printf("****NODE**** reduce job returned, informing master \n"); 
   //send reduce done rpc
  handle h(r_args->primary);
  rpcc *cl = h.safebind();
  if(cl){
     int r;
     int ret = cl->call(node::REDUCE_DONE, r_args->master_id, r_args->job_id, r_args->output_file, r);
   } else {
     tprintf("\nreduce done call failed!\n");
   }
  delete r_args->r;
  delete r_args;
  return 0; 
}


int
node::mapper_done(unsigned master_id, std::string dir,unsigned job_id, int &a)
{
   ScopedLock ml(&map_mutex);
   master_map[master_id]->mapper_done(job_id,dir);
   return 0;
}
int 
node::reducer_done(unsigned master_id,std::string job_id, std::string output_file, int &a)
{
   ScopedLock ml(&map_mutex);
   master_map[master_id]->reducer_done(job_id,output_file);
   return 0;
}

void
node::commit_change(unsigned vid)
{
  ScopedLock ml(&view_mutex);
  ScopedLock ma(&map_mutex);
  std::vector<std::string> c = cfg->get_view(vid);
  std::vector<std::string> p = cfg->get_view(vid - 1);
  VERIFY (c.size() > 0);
   vid_cur = vid;
  if (isamember(primary,c)) {
    tprintf("set_primary: primary stays %s\n", primary.c_str());
    return;
  }

  VERIFY(p.size() > 0);
  for (unsigned i = 0; i < p.size(); i++) {
    if (isamember(p[i], c)) {
      primary = p[i];
      tprintf("set_primary: primary is %s\n", primary.c_str());
      return;
    }
  }

  //set vid on all the masters
  std::map <unsigned,master*>::iterator it;
  for(it= master_map.begin(); it != master_map.end(); it++)
  {
      it->second->set_vid(vid);
  }  
 
}
mapper* 
sort_node::get_mapper(std::string input_file,std::string intermediate_dir)
{
   return new sort_mapper(-10, 10, 10, input_file, intermediate_dir);
}

reducer*
sort_node::get_reducer()
{
   return new sort_reducer();
}
master*
sort_node::get_master(config* cfg, unsigned master_id)
{
   return new sort_master( cfg, master_id);
}
