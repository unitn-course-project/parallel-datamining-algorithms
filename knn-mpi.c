#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include <stdlib.h>
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
double *get_input(int rank, int *local_n, int *n, int *m, int comm_sz, MPI_Comm comm)
{
    //printf("rank %d START get_input \n",rank);
    double *local_a = NULL;
    double *a = NULL;
    int i = 0;
    if (rank == 0)
    {
        printf("Please give n - number of row: ");
        scanf("%d", n);
        printf("Please give m - number of columns: ");
        scanf("%d", m);
        printf("Size of array - %d %d \n", *n, *m);
        if (*n % comm_sz == 0)
            *local_n = *n / comm_sz;
        else
        {
            printf("Please enter a appropiate n with the number of process");
            MPI_Abort(comm, EXIT_FAILURE);
        }
        MPI_Bcast(local_n, 1, MPI_INT, 0, comm);
        MPI_Bcast(m, 1, MPI_INT, 0, comm);
    }
    else
    {

        MPI_Bcast(local_n, 1, MPI_INT, 0, comm);
        MPI_Bcast(m, 1, MPI_INT, 0, comm);
    }
    MPI_Barrier(comm);

    local_a = (double *)malloc(*local_n * *m * sizeof(double));

    if (rank == 0)
    {
        a = (double *)malloc(*n * (*m) * sizeof(double));
        for (i = 0; i < *n; i++)
            for (int j = 0; j < *m; j++)
            {
                printf("Please give the %d %d elenment\n", i, j);
                scanf("%lf", &a[i * *m + j]);
            }
        MPI_Scatter(a, *local_n * *m, MPI_DOUBLE, local_a, *local_n * *m, MPI_DOUBLE, 0, comm);
        free(a);
    }
    else
    {
        MPI_Scatter(a, *local_n * *m, MPI_DOUBLE, local_a, *local_n * *m, MPI_DOUBLE, 0, comm);
    }
    //printf("rank %d END get_input \n ",rank);
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
void kmean(int rank, int k, double local_a[], int m, int local_n, int comm_sz, MPI_Comm comm)
{
    double global_mean[m * k];

    memset(global_mean, 0, sizeof global_mean);
    if (rank == 0)
        for (int i = 0; i < k; i++)
            for (int j = 0; j < m; j++)
            {
                global_mean[i * m + j] = local_a[i * m + j];
            }
    int iterator=0;
    double old_global_mean[m*k];
    while (1)
    {

        printf("rank %d iterator %d \n", rank, iterator);
        MPI_Bcast(global_mean, k * m, MPI_DOUBLE, 0, comm);
        int xd=0;
        for(int i=0;i<m*k;i++)
            if(global_mean[i]!=old_global_mean[i]) {
                xd=1;
                break;
            }
        if(xd==0) break;
        int d_cluster[local_n];
        print_arr(rank, global_mean, m * k, "global mean");
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
        printf("rank %d done found cluster process \n", rank);
        double local_mean[k * m];

        memset(local_mean, 0, sizeof global_mean);
        int number_elm[k];
        memset(number_elm, 0, sizeof global_mean);
        for (int i = 0; i < local_n; i++)
        {
            addVector(&local_mean[d_cluster[i] * m], &local_a[i * m], m);
            number_elm[i] = number_elm[i] + 1;
        }

        for (int i = 0; i < local_n; i++)
        {
            divVector(&local_mean[d_cluster[i] * m], number_elm[i], m);
        }
        printf("rank %d done calculate local mean \n", rank);
        print_arr(rank, local_mean, m * k, "local mean");
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
    if (rank == 0)
        print_arr(rank, global_mean, m * k, "");
}
int main(int argc, char **argv)
{
    int comm_sz;
    int my_rank;
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    // Get the number of processes
    int n;
    int m;
    double *local_a = NULL;
    int local_n = 5;
    local_a = get_input(my_rank, &local_n, &n, &m, comm_sz, MPI_COMM_WORLD);
    int k = 2;
    print_arr(my_rank, local_a, local_n * m, "");
    kmean(my_rank, k, local_a, m, local_n, comm_sz, MPI_COMM_WORLD);
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