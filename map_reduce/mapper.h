#ifndef MAPPER
#define MAPPER

#include <string>

class mapper{
mapper(std::string _input_file, std::string _output_file);
std::string input_file;
std::string output_dir;
unsigned job;
std::string master;
//called by node 
virtual void map();
//old arguments
//std::string input_file, std::string output_dir, unsigned job_id, std::string master_node_id);

virtual void emit_intermediate(std::string key, std::string value);
};

class sort_mapper{
int _min, _max;
unsigned _k;
sort_mapper(int min, int max, unsigned k)
{
    _min = min; 
    _max = max;
    _k = k;
}
 void map(std::string input_file, std::string output_dir, unsigned job_id, std::string master_node_id);

void emit_intermediate(std::string key, std::string value);

};
#endif
