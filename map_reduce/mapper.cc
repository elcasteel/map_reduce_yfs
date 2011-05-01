#include "mapper.h"
#include <iostream>
#include <fstream>

void
sort_mapper::map(std::string input_file, std::string output_dir, unsigned job_id, std::string master_node_id)
{
//open the file
ifstream ifs(input_file,ifstream::in);
//while there is data in the file
   int size = (_max - _min)/_k;
   while(ifs.good())
   {
//read a value
      int cur;
      ifs >> cur;
//decide which bucket
      int bucket = (cur-min)/size;
//put the value in the bucket with emit_intermediate
      std::ostringstream out;
      out <<output_dir.c_str();
      out << "/";
      out << bucket;
      std::ostringstream value_string;
      value_string << cur;
      emit_intermediate(out.str(), value_string.str());
   }
   
   ifs.close();
   
}

void
sort_mapper::emit_intermediate(std::string key, std::string value)
{
    ofstream outfile(key,ofstream::app);
    std::istringstream value_stream(value);
    int value_int;
    value_stream >> value_int;
    outfile << value_int;  
    outfile.close();
   
}

