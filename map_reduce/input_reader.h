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
  off_t bytes;
  off_t curr_offset;

public:
linesplit_input_reader(std::string input_dir,unsigned pieces){
  mydir = input_dir;
  num_pieces = pieces;
  index = 0;

  //store initial files
  DIR *dp;
  struct dirent *dirp;
  if((dp  = opendir(mydir.c_str())) == NULL) {
      VERIFY(0);
  }

  //fill in sizes as well as the file list
  struct stat stFileInfo; 
  int r;
  off_t total = 0;
  while ((dirp = readdir(dp)) != NULL) {
      init_files.push_back(std::string(dirp->d_name));
       r = stat(dirp->d_name,&stFileInfo); 
       VERIFY(r);
       size_map[dirp->d_name] = stFileInfo.st_size;
       total += stFileInfo.st_size;
  }
  closedir(dp);
  bytes = total/pieces;
  curr_offset = 0;
}


std::string get_next_file(){

  if(index >= num_pieces) return "";
  //output file
  std::stringstream ss;
  ss << index;
  std::string name= ss.str();
  std::ofstream outfile (name.c_str());

  



  char *buf;
  off_t dataread =0;
  while(dataread<bytes && init_files.size()){
    //inp file
    std::ifstream stream(init_files.back().c_str());
    
    //check file size
    if(size_map[init_files.back()]-curr_offset < bytes-dataread){
      //read all from offset
      stream.seekg(curr_offset);
      stream.read(buf,size_map[init_files.back()]);
      //increment dataread
      dataread += size_map[init_files.back()];
      //write to file
      outfile << buf;

      //change file & remove old
      stream.close();
      init_files.pop_back();

     //reset offset
     curr_offset = 0;
     
    }else{
      //read bytes-dataread from offset
      stream.seekg(curr_offset);
      stream.read(buf,bytes-dataread);
      //increment data read
      dataread = bytes;
      outfile << buf;
      //read line
      stream.getline(buf,size_map[init_files.back()]);
      outfile << buf;

      //increment curr_offset 
      curr_offset = stream.tellg();
   
    }

    


  }
  index++;
  outfile.close();
  return name;

}

~linesplit_input_reader(){}
};

#endif 
