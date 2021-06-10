#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <dirent.h>
#include <string.h>
#include <sys/types.h>
#include "set.c"
#include <ctype.h>
#include <inttypes.h>
#include <limits.h>

const char* INPUT_DATA_FOLDER = "/home/anhtu/Project/trento/parallel-datamining-algorithms/data/";
const char* TITLE_EXTENSION = "_title.txt";


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

void build_local_dict(int num_file, int my_rank, int comm_sz, SimpleSet *dict, int *total_dict_size){
    FILE *fp;
    //Just read file base on rank
    set_init(dict);
    *total_dict_size = 0;
    for(int i=my_rank; i<num_file; i+=comm_sz){
        //Get file name
        char file_index[sizeof(int)];
        sprintf(file_index, "%d", i);
        char buffer[strlen(INPUT_DATA_FOLDER)+sizeof(int)+strlen(TITLE_EXTENSION)];
        strcat(strcpy(buffer, INPUT_DATA_FOLDER), file_index);
        strcat(buffer, TITLE_EXTENSION);
        // printf("Rank %d Read file %s \n", my_rank, buffer);
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
                    if(set_contains(dict, word_resized) == SET_FALSE){
                        set_add(dict, word_resized);
                        *total_dict_size += (cnt+1);
                    }
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
            if(set_contains(dict, word_resized) == SET_FALSE){
                set_add(dict, word_resized);
                *total_dict_size += (cnt+1);
            }
        }
    }
}


// void merge_dict(void* input, void* output, int* len, MPI_Datatype* datatype){
//     SimpleSet merged_dict;

//     set_init(&merged_dict);
//     for(int i=0; i<sizeof(input); i++){

//     }
// }

int main(void)
{
    int my_rank, comm_sz;
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);

    int num_file = 0;
    
    //Count number of file
    if (my_rank == 0){
        num_file = count_files(INPUT_DATA_FOLDER)/3;
        printf("Number of file is: %d\n", num_file);
    }
    MPI_Bcast(&num_file, 1, MPI_INT, 0, MPI_COMM_WORLD);

    int total_dict_size;
    SimpleSet set_local_dict;
    build_local_dict(num_file, my_rank, comm_sz, &set_local_dict, &total_dict_size);
    // print_set(set_local_dict, my_rank);
    // printf("Rank %d Total dict size = %d\n", my_rank, total_dict_size);
    
    uint64_t dict_size;
    char** local_dict = set_to_array(&set_local_dict, &dict_size);

    char** global_dict = NULL;
    if(my_rank %2 == 0){
        // memcpy(global_dict, local_dict, total_dict_size);
        MPI_Status* status = NULL;
        uint64_t num_word;
        MPI_Recv(&num_word, 1, MPI_UINT64_T, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("Rank %d receive: num_word = %ld\n",my_rank, num_word);
        // MPI_Recv(local_dict, INT_MAX, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, status);
        // printf("Rank %d receive: ", my_rank);
        // for(int i=0; i < num_word; i++){
        //     printf("%s ", local_dict[i]);
        // }
        // printf("\n");
        //TO-DO MERGE LOCAL AND GLOBAL DICT
        // int mess_count;
        // MPI_Get_count(status, MPI_CHAR, &mess_count);
        // total_dict_size += mess_count;
        dict_size += num_word;
        if(my_rank != 0){
            if(my_rank % 4 == 0){
                printf("Rank %d send dict_size = %ld\n", my_rank, dict_size);
                MPI_Send(&dict_size, 1, MPI_UINT64_T, my_rank-4, my_rank, MPI_COMM_WORLD);
                // MPI_Send(local_dict, total_dict_size, MPI_CHAR, my_rank-4, my_rank, MPI_COMM_WORLD);
            }else{
                printf("Rank %d send dict_size = %ld\n", my_rank, dict_size);
                MPI_Send(&dict_size, 1, MPI_UINT64_T, my_rank-2, my_rank, MPI_COMM_WORLD);
                // MPI_Send(local_dict, total_dict_size, MPI_CHAR, my_rank-2, my_rank, MPI_COMM_WORLD);
            }
        }
    }else {
        printf("Rank %d send dict_size = %ld\n", my_rank, dict_size);
        MPI_Send(&dict_size, 1, MPI_UINT64_T, my_rank-1, my_rank, MPI_COMM_WORLD);
        // printf("Rank %d send local dict with size = %d", my_rank, total_dict_size);
        // MPI_Send(local_dict, total_dict_size, MPI_CHAR, my_rank-1, my_rank, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}