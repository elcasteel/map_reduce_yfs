#include "../rpc/rpc.h"
#include "../tprintf.h"
#include "../config.h"
#include <string>
#include <pthread.h>

class node:config_view_change{

private:
config *cfg;
std::string primary;
std::string myid;
pthread_mutex_t view_mutex;

 
public:
//rpc enums
enum CALL {JOB };
enum job_type{ MAP,REDUCE, MAP_REDUCE};


virtual mapper get_mapper() = 0;
virtual reducer get_reducer() =0;
void commit_view_change(unsigned vid);
void start_map(std::string initial_data, std::string intermediate_dir);
void start_reduce(std::string input_file,std::string output_file);
//rpc handler
int new_job(job_type t, std::string input,unsigned job_id, std::string master_id);
class *master m;
bool amiprimary();
}

