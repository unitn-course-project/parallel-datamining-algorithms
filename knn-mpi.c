#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
double f(double x)
{
    return x * x;
}

void print_arr(int rank, double *x, int size)
{
    printf("Print from rank %d size %d \n", rank, size);
    int i = 0;
    for (i = 0; i < size; i++)
    {
        printf("%f ", x[i]);
    }
}
double* get_input(int rank, int *local_n, int *n, int *m, int comm_sz, MPI_Comm comm)
{
    //printf("rank %d START get_input \n",rank);
    double *local_a=NULL;
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
    printf("From rank %d local_n %d m %d",rank,*local_n,*m);
    print_arr(rank, local_a, *local_n**m);
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
    int local_n=5;

    local_a=get_input(my_rank, &local_n, &n, &m, comm_sz, MPI_COMM_WORLD);

    print_arr(my_rank, local_a, local_n*m);
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