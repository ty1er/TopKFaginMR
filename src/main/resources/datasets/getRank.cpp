#include <iostream>
#include <string>
#include <vector>
#include <fstream>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <iomanip>
using namespace std;

int main()
{
  
  int flag;
  int obj;
  double val;
  string line;
  string str;
  ifstream infile2("dataset1.txt");
  ofstream outfile2("data.txt");



  while(getline(infile2, str)){
    ofstream outfile("tmp.txt");
    outfile << str << endl;
    val = 0.0;
    flag = 1;
    ifstream infile("tmp.txt");
    while(getline(infile, line, '\t' )){
      if(flag){
	obj = atoi(line.c_str());
	flag = 0;
	outfile2 << obj << '\t';
      }
      else{
	val += atof(line.c_str());
      }
    }
    val += 5.0;
    outfile2 << setprecision(10) << val << endl;

    infile.close();
    outfile.close();
  }

  infile2.close();
  outfile2.close();

  return 0;
}
