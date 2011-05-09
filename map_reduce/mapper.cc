#include "mapper.h"
#include <iostream>
#include <fstream>
//#include <ifstream>
#include <sstream>

void
sort_mapper::map()
//old arguments
//std::string input_file, std::string output_dir, unsigned job_id, std::string master_node_id)
{
//open the file
std::ifstream ifs(input_file.c_str());
//while there is data in the file
   int size = (_max - _min)/_k;
   while(ifs.good())
   {
//read a value
      int cur;
      ifs >> cur;
//decide which bucket
      int bucket = (cur-_min)/size;
//put the value in the bucket with emit_intermediate
      std::stringstream out(std::stringstream::in);
      out <<output_dir.c_str();
      out << "/";
      out << bucket;
      std::stringstream value_string(std::stringstream::in);
      value_string << cur;
      emit_intermediate(out.str(), value_string.str());
   }
   
   ifs.close();
   
}

void
sort_mapper::emit_intermediate(std::string key, std::string value)
{
    std::ofstream outfile(key.c_str(),std::ofstream::app);
    std::istringstream value_stream(value);
    int value_int;
    value_stream >> value_int;
    outfile << value_int;  
    outfile.close();
   
}

