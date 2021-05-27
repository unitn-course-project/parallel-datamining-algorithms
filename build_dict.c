#include<stdio.h>
#include<stdlib.h>
#include<mpi.h>
#include<dirent.h>
#include<string.h>
#include<sys/types.h>

int count_files(const char *folder_path)
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
    int my_rank, comm_sz;
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);

    int num_file = 0;
    const char* input_data_folder = "/media/anhtu/046AAF9E6AAF8B4E/trento/archive/document_parses/txt_file/";
    const char* title_extension = "_title.txt";

    if (my_rank == 0){
        num_file = count_files(input_data_folder)/3;
        printf("Number of file is: %d", num_file);
    }
    MPI_Bcast(&num_file, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (my_rank != 0){
        FILE *fp;
        for(int i=my_rank; i<num_file; i+=comm_sz){
            char file_index[sizeof(int)];
            sprintf(file_index, "%d", i);
            char buffer[strlen(input_data_folder)+sizeof(int)+strlen(title_extension)];
            strcat(strcpy(buffer, input_data_folder), file_index);
            strcat(buffer, title_extension);
            fp = fopen(buffer, "r");
            char ch;
            while ((ch = fgetc(fp)) != EOF)
                printf("%c", ch);
        }
    }
    MPI_Finalize();
    return 0;
}