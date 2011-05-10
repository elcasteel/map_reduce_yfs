#include "node.h"
#include <dirent.h>
#include "../handle.h"


node::node(std::string first,std::string me){
  //init config
  myid = me;
  primary = first;
  cfg = new config(first,me,this);
  
  if(first ==me)
  {
     commit_change(1);
  }
  
  //init mutexes, etc
  pthread_mutex_init(&view_mutex,NULL);
  pthread_mutex_init(&map_mutex, NULL);
  //register rpc handlers
  rpc_server = cfg ->get_rpcs();
  rpc_server->reg(node::MAP, this ,&node::start_map);
  rpc_server->reg(node::REDUCE, this, &node::start_reduce);
  rpc_server->reg(node::MAP_REDUCE, this ,&node::start_map_reduce);
  rpc_server->reg(node::MAP_DONE,this,&node::mapper_done);
  rpc_server->reg(node::REDUCE_DONE,this,&node::reducer_done);
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
node::start_map(std::string input_file, unsigned job_id, unsigned master_id, int &a)
{
  //choose a file name for the output
  stringstream ss(stringstream::in);
  ss << input_file;
  ss << "-";
  ss << job_id;
  std::string intermediate_dir = ss.str();
  int r= mkdir(intermediate_dir.c_str(), 0777);
  VERIFY(r == 0);
  //setup a new thread and mapper object
  pthread_t th;
  mapper* m =get_mapper(input_file, intermediate_dir);
  struct do_map_args args;
  args.m= m;
  args.job_id = job_id;
  args.master_id = master_id; 
  args.output_dir = intermediate_dir;
  args.primary = primary;
  r = pthread_create(&th,NULL,&do_map,&args );
  VERIFY(r == 0); 
  //return 
  return 0;
}
void *
do_map(void* _args)
{
  struct do_map_args* args = (do_map_args*) _args;
  args->m->map();
  handle h(args->primary);
  rpcc *cl = h.safebind();
  if(cl){
     int r;
     int ret = cl->call(node::MAP_DONE,args->master_id, args->output_dir, args->job_id, r);
   } else {
     tprintf("\nmap done call failed!\n");
   }
   delete args->m;
   
   return 0;
}

int 
node::start_reduce(std::string file_list, std::string job_id,unsigned master_id, int &a)
{
    //chose a file name for the output
    stringstream outstream;
    outstream << "./";
    outstream << job_id;
    outstream << ".";
    outstream << master_id;
    outstream << ".out";
    std::string output_file = outstream.str();
       
   //set up a new thread and reducer
   reducer *r = get_reducer();
   struct do_reduce_args args;
   args.r =r; 
   args.job_id = job_id;
   args.master_id = master_id; 
   args.output_file = output_file;
   args.file_list = file_list;
   args.primary = primary;
   pthread_t th;
   pthread_create(&th,NULL, &do_reduce,&args);
   return 0;
}
void *
do_reduce(void *args)
{
   struct do_reduce_args *r_args = (do_reduce_args*) args;
   //call reduce
   r_args->r->start_reduce(r_args->file_list,r_args->output_file);
   
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
   return new sort_mapper(-100000, 100000, 10, input_file, intermediate_dir);
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
