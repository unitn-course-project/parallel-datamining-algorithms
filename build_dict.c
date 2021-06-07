#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <dirent.h>
#include <string.h>
#include <sys/types.h>
#include "set.c"
#include <ctype.h>
#include <inttypes.h>


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

void print_set(SimpleSet set, int rank){
    uint64_t set_size;
    char** array = set_to_array(&set, &set_size);
    printf("Rank %d Set size = %" PRIu64 " Set Value: ", rank, set_size);
    for(int i=0; i < set_size; i++){
        printf("%s ", array[i]);
    }
    printf("\n");
}


int main(void)
{
    int my_rank, comm_sz;
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);

    int num_file = 0;
    const char* input_data_folder = "/home/anhtu/Project/trento/parallel-datamining-algorithms/data/";
    const char* title_extension = "_title.txt";

    //Count number of file
    if (my_rank == 0){
        num_file = count_files(input_data_folder)/3;
        printf("Number of file is: %d", num_file);
    }
    MPI_Bcast(&num_file, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (my_rank != 0){
        FILE *fp;
        //Just read file base on rank 
        SimpleSet dict;
        set_init(&dict);
        for(int i=(my_rank-1); i<num_file; i+=(comm_sz-1)){
            //Get file name
            char file_index[sizeof(int)];
            sprintf(file_index, "%d", i);
            char buffer[strlen(input_data_folder)+sizeof(int)+strlen(title_extension)];
            strcat(strcpy(buffer, input_data_folder), file_index);
            strcat(buffer, title_extension);
            printf("Rank %d Read file %s \n", my_rank, buffer);
            //Read file
            fp = fopen(buffer, "r");

            char ch;
            //store word
            char* word = malloc(50* sizeof(char));
            int cnt = 0;
            while ((ch = fgetc(fp)) != EOF){
                ch = tolower(ch);
                //read word by word and add to dict
                if(ch == ' ' || ch == '\n'){
                    if(cnt != 0){
                        word[cnt] = '\0';
                        char* word_resized = realloc(word, (cnt+1)*sizeof(char));
                        set_add(&dict, word_resized);
                        cnt = 0;
                        word = malloc(50*sizeof(char));
                    }
                }else{
                    word[cnt] = ch;
                    cnt++;
                }
            }
            if(cnt != 0){
                word[cnt] = '\0';
                char* word_resized = realloc(word, (cnt+1)*sizeof(char));
                set_add(&dict, word_resized);
                cnt = 0;
                word = malloc(50*sizeof(char));
            }
        }
        print_set(dict, my_rank);
    }
    MPI_Finalize();
    return 0;
}