#ifndef NODE
#define NODE

#include "../rpc/rpc.h"
#include "../tprintf.h"
#include "../config.h"
#include <string>
#include <pthread.h>
#include "../threaded_queue.h"
#include "../paxos.h"
#include "mapper.h"
#include "reducer.h"
#include "master.h"
#include <map>
#include "../rsm_protocol.h"

class node:public config_view_change{

protected:
config *cfg;
std::string primary;
std::string myid;
pthread_mutex_t view_mutex;
rpcs* rpc_server;
viewstamp myvs;
viewstamp last_myvs;
unsigned vid_cur;
public:

rsm_protocol::status joinreq(std::string src, 
			       rsm_protocol::joinres &r);
bool join(std::string m);

//rpc enums
//enum CALL {JOB };
enum job_type{ MAP=0x10001,REDUCE, MAP_REDUCE,MAP_DONE,REDUCE_DONE,JOINREQ};
 
struct rpc_call{
        
        job_type type;
        std::string input;
        std::string master_id;
        unsigned job_id;
  };

threaded_queue<rpc_call> rpc_queue;


unsigned last_master_id;
pthread_mutex_t map_mutex; 

virtual mapper* get_mapper(std::string input_file, std::string intermediate_dir) = 0;
virtual reducer* get_reducer() =0;
virtual master* get_master(config* cfg, unsigned master_id)=0;
node(std::string first,std::string me);
void commit_change(unsigned vid);
virtual ~node(){};
//rpc handlers

int start_map(std::string initial_data, unsigned job_id,unsigned master_id, int &a);
int start_reduce(std::string file_list, std::string job_id, unsigned master_id,std::string output_dir, int &a);
int start_map_reduce(std::string input_file, std::string output_file,int &a);

int mapper_done(unsigned master_id, std::string dir,unsigned job_id, int &a);
int reducer_done(unsigned master_id,std::string job_id, std::string output_file, int &a); 
//rpc handler
//int new_job(job_type t, std::string input,unsigned job_id, std::string master_id);


std::map<unsigned,master*> master_map;
//bool amiprimary();




};

struct do_map_args
{
   mapper *m; 
   unsigned job_id; 
   unsigned master_id;
   std::string output_dir;
   std::string primary;

};
struct do_reduce_args
{
   reducer *r;
   std::string job_id;
   unsigned master_id;
   std::string output_file; 
   std::string file_list;
   std::string primary;
};


void* do_map(void *args);
void* do_reduce(void *args);


class sort_node:public node
{
mapper* get_mapper(std::string input_file, std::string intermediate_dir);
reducer* get_reducer();
master* get_master(config *cfg, unsigned master_id);
public:
sort_node(std::string first,std::string me):node(first,me){}
~sort_node(){}

};


#endif
