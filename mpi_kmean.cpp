#include <iostream>
#include <fstream>
#include <string>
#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <stdlib.h>
using namespace std;
const string inputFolderPath = "output/vector/";
const string inputFileType = ".data";
double f(double x)
{
  return x * x;
}

void print_arr(int rank, double *x, int size, char *msg)
{
  printf("Print from rank %d message %s \n", rank, msg);
  printf("Print from rank %d size %d \n", rank, size);
  int i = 0;
  for (i = 0; i < size; i++)
  {
    printf("%f ", x[i]);
  }
  printf("End arr \n");
}
void print_arr_int(int rank, int *x, int size, char *msg)
{
  printf("Print from rank %d message %s \n", rank, msg);
  printf("Print from rank %d size %d \n", rank, size);
  int i = 0;
  for (i = 0; i < size; i++)
  {
    printf("%d ", x[i]);
  }
  printf("End arr \n");
}
double *get_array_from_file(int rank, string fileName)
{
  streampos size;
  int *memblock;

  ifstream file(inputFolderPath + fileName, ios::in | ios::binary | ios::ate);
  if (file.is_open())
  {
    size = file.tellg();
    memblock = new int[size / sizeof(int)];
    file.seekg(0, ios::beg);
    file.read((char *)memblock, size);
    file.close();
    //cout << memblock[0] << endl;
    double *tmp = new double[size / sizeof(int)];
    int count = 0;
    for (int i = 0; i < size / sizeof(int); i++)
    {
      tmp[i] = memblock[i] * 1.0;
      if (tmp[i] != -1)
        count++;
    }
    //cout <<fileName<<" number different -1 is "<< count<<endl;
    delete[] memblock;
    //print_arr(rank, tmp, size / sizeof(int), " input tmp ");
    return tmp;
  }
  else
    cout << "Unable to open file";
  return NULL;
}

double *get_input(int rank, int *local_n, int *n, int *m, int comm_sz, MPI_Comm comm)
{
  double *local_a = new double[*local_n * *m];
  for (int i = 0; i < *local_n; i++)
  {
    string fileName = to_string(rank * *local_n + i) + inputFileType;
    cout << "print from rank " << rank << "read file" << fileName << endl;
    double *tmp = get_array_from_file(rank, fileName);
    if (tmp != NULL)
      memcpy(&local_a[i * *m], tmp, *m * sizeof(double));
  }

  //print_arr(rank, local_a, *m * *local_n, " input ");
  return local_a;
}

double *get_output(int rank, int n, int local_n, double local_y[], MPI_Comm comm)
{
  //printf("rank %d START get_out \n ",rank);
  double *y = NULL;
  if (rank == 0)
  {
    //printf("this is local_n %d",local_n);

    y = (double *)malloc(n * sizeof(double));
    MPI_Gather(local_y, local_n, MPI_DOUBLE, y, local_n, MPI_DOUBLE, 0, comm);
  }
  else
  {
    MPI_Gather(local_y, local_n, MPI_DOUBLE, y, local_n, MPI_DOUBLE, 0, comm);
  }
  //printf("rank %d END get_input \n ",rank);
  return y;
}

double distance(double x[], double y[], int l_n)
{
  double s = 0;
  for (int i = 0; i < l_n; i++)
    s += (x[i] - y[i]) * (x[i] - y[i]);
  return sqrt(s);
}

void addVector(double x[], double y[], int l_n)
{
  for (int i = 0; i < l_n; i++)
  {
    x[i] = x[i] + y[i];
  }
}
void divVector(double x[], double dividend, int l_n)
{
  for (int i = 0; i < l_n; i++)
  {
    x[i] = x[i] / dividend;
  }
}
int *kmean(int max_iterator, int rank, int k, double local_a[], int m, int n, int local_n, int comm_sz, MPI_Comm comm)
{
  //print_arr(rank, local_a, m * local_n, "start kmean");
  double global_mean[m * k];

  memset(global_mean, 0, sizeof global_mean);
  if (rank == 0)
    for (int i = 0; i < k; i++)
      for (int j = 0; j < m; j++)
      {
        global_mean[i * m + j] = local_a[i * m + j];
      }
  int iterator = 0;
  double old_global_mean[m * k];
  while (1)
  {

    printf("rank %d iterator %d \n", rank, iterator);
    MPI_Bcast(global_mean, k * m, MPI_DOUBLE, 0, comm);
    int xd = 0;
    for (int i = 0; i < m * k; i++)
      if (global_mean[i] != old_global_mean[i])
      {
        xd = 1;
        break;
      }
    if (xd == 0 || iterator > max_iterator)
      break;
    int d_cluster[local_n];
    int use_cluster[k];

    memset(use_cluster, 0, sizeof use_cluster);
    //print_arr(rank, global_mean, m * k, "global mean");
    for (int i = 0; i < local_n; i++)
    {
      double min_distance = 10000000;
      int cluster_index = -1;
      for (int j = 0; j < k; j++)
      {
        double dis = distance(&local_a[i * m], &global_mean[j * m], m);
        if (dis < min_distance)
        {
          min_distance = dis;
          cluster_index = j;
        }
      }
      d_cluster[i] = cluster_index;
      use_cluster[cluster_index] = 1;
    }
    printf("rank %d done found cluster process \n", rank);
    double local_mean[k * m];

    memset(local_mean, 0, sizeof global_mean);
    int number_elm[k];
    memset(number_elm, 0, sizeof number_elm);
    for (int i = 0; i < local_n; i++)
    {
      addVector(&local_mean[d_cluster[i] * m], &local_a[i * m], m);
      number_elm[d_cluster[i]] = number_elm[d_cluster[i]] + 1;
    }
    //print_arr(rank, local_mean, m * k, "local mean not div");
    //print_arr_int(rank, number_elm, k, "number elm ");
    for (int i = 0; i < k; i++)
    {
      divVector(&local_mean[i * m], number_elm[i], m);
    }
    for (int i = 0; i < k; i++)
    {
      if (use_cluster[i] == 0)
        for (int j = 0; j < m; j++)
          local_mean[i * m + j] = global_mean[i * m + j];
    }
    printf("rank %d done calculate local mean \n", rank);
    //print_arr(rank, local_mean, m * k, "local mean");
    if (rank != 0)
      MPI_Send(local_mean, k * m, MPI_DOUBLE, 0, 0, comm);
    memcpy(old_global_mean, global_mean, sizeof(global_mean));
    memset(global_mean, 0, sizeof global_mean);
    addVector(global_mean, local_mean, k * m);
    if (rank == 0)
    {
      for (int i = 1; i < comm_sz; i++)
      {
        printf("rank %d wait recv from process %d \n", rank, i);
        double local_mean_tmp[m * k];
        MPI_Recv(local_mean_tmp, m * k, MPI_DOUBLE, i, 0, comm, MPI_STATUS_IGNORE);
        addVector(global_mean, local_mean_tmp, k * m);
      }
      for (int i = 0; i < m * k; i++)
        global_mean[i] = global_mean[i] / comm_sz;
    }
    printf("rank %d done propagate local mean", rank);
    iterator++;
  }
  int d_cluster[local_n];
  //print_arr(rank, global_mean, m * k, "global mean");
  for (int i = 0; i < local_n; i++)
  {
    double min_distance = 10000000;
    int cluster_index = -1;
    for (int j = 0; j < k; j++)
    {
      double dis = distance(&local_a[i * m], &global_mean[j * m], m);
      if (dis < min_distance)
      {
        min_distance = dis;
        cluster_index = j;
      }
    }
    d_cluster[i] = cluster_index;
  }
  print_arr_int(rank, d_cluster, local_n, "d cluster");
  int *total_d_cluster = NULL;
  if (rank == 0)
  {
    total_d_cluster = new int[n];
  }
  MPI_Gather(&d_cluster, local_n, MPI_INT, total_d_cluster, local_n, MPI_INT, 0,
             MPI_COMM_WORLD);

  if (rank == 0)
    print_arr(rank, global_mean, m * k, "");
  return total_d_cluster;
}
int main(int argc, char **argv)
{
  cout << "STRART" << endl;
  int comm_sz;
  int my_rank;
  MPI_Init(NULL, NULL);
  MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
  MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
  // Get the number of processes
  int max_iterator = 1000;
  int n = 1000;
  int m = 300;
  double *local_a = NULL;
  int local_n = n/comm_sz;
  local_a = get_input(my_rank, &local_n, &n, &m, comm_sz, MPI_COMM_WORLD);
  int k = 4;
  //print_arr(my_rank, local_a, local_n * m, "");
  int *total_d_cluster = kmean(max_iterator, my_rank, k, local_a, m, n, local_n, comm_sz, MPI_COMM_WORLD);
  if (my_rank == 0)
    print_arr_int(my_rank, total_d_cluster, n, " cluster per element");
  // double *y= NULL;
  // double *local_y=(double *) malloc(local_n * sizeof(double));
  // int i = 0;
  // for(i=0;i<local_n;i++){
  //     local_y[i]=f(local_a[i]);
  // }
  // y=get_output(my_rank,n,local_n,local_y,MPI_COMM_WORLD);

  // if(my_rank==0) print_arr(my_rank, y, n);
  //for (i = 0; i < n; i++)
  //{
  //    printf("%f ", y[i]);
  //}

  MPI_Finalize();
  return 0;
}