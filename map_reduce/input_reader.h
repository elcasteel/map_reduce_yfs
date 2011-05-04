#include <string>
#include <vector>
#include <dirent.h>
#include "lang/verify.h"
#include <iostream>
#include <fstream>

class input_reader{
protected:
std::string mydir;
//keep track of how many pieces
unsigned index, num_pieces;

public:
input_reader(std::string input_dir,unsigned pieces);
virtual std::string get_next_file();


}

class linesplit_input_reader:input_reader{
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
  if((dp  = opendir(dir.c_str())) == NULL) {
      cout << "Error(" << errno << ") opening " << dir << endl;
      VERIFY(0);
  }

  //fill in sizes as well as the file list
  struct stat stFileInfo; 
  int r;
  off_t total = 0;
  while ((dirp = readdir(dp)) != NULL) {
      init_files.push_back(string(dirp->d_name));
       r = stat(dirp->d_name,&stFileInfo); 
       VERIFY(r);
       size_map[dirp->d_name] = stFileInfo.st_size;
       total += stFileInfo.st_size;
  }
  closedir(dp);
  bytes = total/pieces;
  curr_offset = 0;
}


virtual std::string get_next_file(){

  if(index >= pieces) return NULL;
  //output file
  stringstream ss;
  ss << index;
  std::string name= ss.string();
  ofstream outfile (name);

  



  char *buf;
  off_t dataread =0;
  while(dataread<bytes && init_files.size()){
    //inp file
    filebuf file;
    file.open(init_files.back(),ios::in);
    ifstream stream(&file);
    
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
      file.close()
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
      outfile << buff;

      //increment curr_offset 
      curr_offset = stream.tellg();
   
    }

    


  }
  index++;
  outfile.close();
  return name;

}


}
