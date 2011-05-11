#ifndef INPUT_READER
#define INPUT_READER

#include <string>
#include <vector>
#include <dirent.h>
#include "../lang/verify.h"
#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <sstream>
#include "../tprintf.h"
class input_reader{
protected:
std::string mydir;
//keep track of how many pieces
unsigned index, num_pieces;

public:
virtual std::string get_next_file(){VERIFY(0);}
virtual ~input_reader(){};

};

class linesplit_input_reader:public input_reader{
private:
  std::vector<std::string> init_files;
  std::map<std::string,off_t> size_map;
  unsigned bytes;
  int curr_offset;
  std::string _output_dir;

public:
linesplit_input_reader(std::string input_dir,unsigned pieces,std::string output_dir){
  mydir = input_dir;
  num_pieces = pieces;
  index = 0;
  _output_dir = output_dir;
  //store initial files
  DIR *dp;
  struct dirent *dirp;
  if((dp  = opendir(mydir.c_str())) == NULL) {
      VERIFY(0);
  }

  //fill in sizes as well as the file list
  //struct stat stFileInfo; 
  int r;
  unsigned off_total = 0;
  while ((dirp = readdir(dp)) != NULL) {
      std::string fname = mydir+"/"+std::string(dirp->d_name); 
      init_files.push_back(fname);
       //r = stat(dirp->d_name,&stFileInfo); 
       //VERIFY(r);
       //size_map[fname] = stFileInfo.st_size;
       //off_total += stFileInfo.st_size;
      std::ifstream is;
      is.open(fname.c_str(),std::ios::binary);
      //get length of file
      is.seekg(0,std::ios::end);
      size_map[fname] = is.tellg();
      off_total += is.tellg();
  }
  printf("****INPUT_READER****\nfound %d files \n", init_files.size());
  closedir(dp);
  unsigned total = off_total;
  bytes = total/pieces;
  printf("****INPUT_READER**** setting bytes %u total %u \n",bytes,total);
  printf("****INPUT_READER**** off_total %u \n",off_total);
  curr_offset = 0;

  //make dir
  r= mkdir(output_dir.c_str(), 0777);
  VERIFY(r == 0);
}


std::string get_next_file(){
  printf("****INPUT_READER****\n input reader index %d num_pieces %d \n",index, num_pieces);
  
  if(index >= num_pieces) return "";
  //output file
  std::stringstream ss;
  ss << _output_dir;
  ss << "/";
  ss << index;
  std::string name= ss.str();
  std::ofstream outfile (name.c_str());

  printf("****INPUT_READER****\nget next file: %s \n", name.c_str());
  


  std::string line;
  off_t dataread =0;

  while(dataread<bytes && init_files.size()){
    //inp file
    printf("****INPUT_READER**** opening %s \n",init_files.back().c_str());
    std::ifstream stream(init_files.back().c_str());
    printf("****INPUT_READER**** opened input file\n"); 
    stream.seekg(curr_offset,std::ios::beg);
    while(stream.good() && dataread < bytes){
      //get a line from this file
      std::getline(stream,line);
      dataread += (int) stream.tellg()-curr_offset;
      curr_offset = stream.tellg();      
      outfile << line.c_str();
      outfile << "\n";
    }
    if(!stream.good()){
       //we are done with this file
       curr_offset = 0;
       init_files.pop_back();

    }
    stream.close();
   

    


  }
  //outfile << "\n";
  index++;
  outfile.close();
  return name;

}

~linesplit_input_reader(){}
};

#endif 
