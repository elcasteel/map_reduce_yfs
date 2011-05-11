#include "mapper.h"
#include <iostream>
#include <fstream>
//#include <ifstream>
#include <sstream>
#include <stdlib.h>
void
sort_mapper::map()
//old arguments
//std::string input_file, std::string output_dir, unsigned job_id, std::string master_node_id)
{
//open the file
  printf("****MAPPER**** starting map %s %s\n",input_file.c_str(),output_dir.c_str());
  fflush(stdout);
std::ifstream ifs(input_file.c_str());
//while there is data in the file
   int size = (_max - _min)/_k;
   while(ifs.good())
   {
//read a value
      int cur;
      //ifs >> cur;
      std::string line;
      std::getline(ifs,line);
      
      std::stringstream convert(line);
      convert >> cur;
      int check = atoi(line.c_str());
      if(check != cur || line == ""){
        printf("***MAPPER*** got a bad curr\n");

        continue;
      }
//decide which bucket
      
      int bucket = (cur-_min)/size;
      printf("cur %d is going into bucket %d with line %s\n",cur,bucket,line.c_str());
//put the value in the bucket with emit_intermediate
      std::stringstream out(std::stringstream::in|std::stringstream::out);
      out <<output_dir.c_str();
      out << "/";
      out << bucket;
      std::stringstream value_string(std::stringstream::in|std::stringstream::out);
      value_string << cur;
      emit_intermediate(out.str(), value_string.str());
   }
  printf("****MAP**** done mapping\n"); 
   ifs.close();
   
}

void
sort_mapper::emit_intermediate(std::string key, std::string value)
{
    std::ofstream outfile(key.c_str(),std::ofstream::app);
    int value_int = atoi(value.c_str());
    printf("****MAPPER**** printing value to file %s %d\n",key.c_str(),value_int);
    //value_stream >> value_int;
    outfile << value_int;  
    outfile << "\n";
    outfile.close();
   
}

