#include <iostream>
#include <fstream>
#include <filesystem>
namespace fs = std::filesystem;
using namespace std;
string cluster_path = "cluster/";
string cluster_file = "mpi_kmean.txt";
void show_help()
{
  cout << "Please use -h or --help to get information about argument" << endl;
  cout << "-clusterPath   path to indicate out folder" << endl;
  cout << "-clusterFile *  solution file of k mean step" << endl;
}
int main(int argc, char **argv)
{
  int mandatory_field = 0;
  for (int i = 1; i < argc; ++i)
  {
    string arg = argv[i];
    if ((arg == "-h") || (arg == "--help"))
    {
      show_help();
    }
    if ((arg == "-clusterPath") || (arg == "--clusterPath"))
    {
      cluster_path = argv[i + 1];
    }
    if ((arg == "-clusterFile") || (arg == "--clusterFile"))
    {
      cluster_file = argv[i + 1];
      mandatory_field++;
    }
  }
  if (mandatory_field < 1)
  {
    cout << "Please set value for mandatory field!" << endl;
    return 1;
  }
  ifstream file(cluster_file, ios::in);
  if (file.is_open())
  {
    while (!file.eof())
    {
      string line;
      getline(file, line);
      string delimiter = " ";
      int pos = line.find(delimiter);
      if (pos != string::npos)
      {
        string path = line.substr(0, pos);
        string cluster = line.substr(pos + 1, line.length() - pos);
        cout << "path is |" << path << "| cluster |" << cluster << "|" << endl;

        string filename;
        int pos_2 = path.find_last_of("/");

        if (pos_2 != string::npos)
          filename = path.substr(pos_2, path.length() - pos_2);
        else
          filename = pos;
        cout << "file name " << filename << endl;
        fs::create_directories(cluster_path + cluster);
        fs::copy(path, cluster_path + cluster + filename);
      }
    }
  }
  else
    cout << "Unable to open file";
  return 0;
}