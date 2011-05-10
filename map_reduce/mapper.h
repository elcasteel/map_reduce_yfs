#ifndef MAPPER
#define MAPPER

#include <string>

class mapper{
protected:
std::string input_file;
std::string output_dir;
unsigned job;
std::string master;
public:
//called by node 
virtual void map(){}
//old arguments
//std::string input_file, std::string output_dir, unsigned job_id, std::string master_node_id);
virtual ~mapper(){}
mapper(){}
virtual void emit_intermediate(std::string key, std::string value){}
};

class sort_mapper:public mapper{
int _min, _max;
unsigned _k;

public:
sort_mapper(int min, int max, unsigned k, std::string _input_file, std::string _output_dir):mapper()
{
    _min = min; 
    _max = max;
    _k = k;
   input_file = _input_file;
   output_dir = _output_dir;

}
void map();
void emit_intermediate(std::string key, std::string value);
~sort_mapper(){}

};
#endif
