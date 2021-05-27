#include<stdio.h>
#include<stdlib.h>
#include<mpi.h>
#include<dirent.h>
#include<sys/types.h>


int count_files(char *folder_path)
{
    int file_count = 0;
    DIR *dirp;
    struct dirent * entry;
    
    dirp = opendir(folder_path);
    while((entry = readdir(dirp)) != NULL){
        if (entry->d_type == DT_REG && entry->d_name[0] != '.')
        {
            file_count++;
        }
        
    }
    closedir(dirp);
    return file_count;
}


int main(void)
{
    // int my_rank;
    // MPI_Init(NULL, NULL);
    // MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    char* input_data_folder = "/home/anhtu/Project/trento/HPC4DS/HPC4DS_exercise";
    int num_file = count_files(input_data_folder);
    printf("Number of file is: %d", num_file);
    return 0;
}