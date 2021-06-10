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


static int fastlog2(uint32_t v) {
    // http://graphics.stanford.edu/~seander/bithacks.html
    int r;
    static const int MultiplyDeBruijnBitPosition[32] = 
    {
    0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30,
    8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31
    };

    v |= v >> 1; // first round down to one less than a power of 2 
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;

    r = MultiplyDeBruijnBitPosition[(uint32_t)(v * 0x07C4ACDDU) >> 27];
    return r;
}

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

void print_array(char** array, uint64_t size){
    for(int i=0; i<size; i++){
        printf("%s ", array[i]);
    }
    printf("\n");
}

void build_local_dict(int num_file, int my_rank, int comm_sz, SimpleSet *dict){
    FILE *fp;
    //Just read file base on rank
    set_init(dict);
    // *total_dict_size = 0;
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
                        // *total_dict_size += (cnt+1);
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
                // *total_dict_size += (cnt+1);
            }
        }
    }
}


char** merge_dict(char** src, uint64_t src_size, char** dst, uint64_t dst_size, uint64_t* dict_size, int my_rank){
    printf("Rank %d merge two dict %ld and %ld\n", my_rank, src_size, dst_size);
    // printf("Source array: ");
    // print_array(src, src_size);
    // printf("Destination array: ");
    // print_array(dst, dst_size);

    SimpleSet s_final_dict;
    set_init(&s_final_dict);
    for(int i=0; i<src_size; i++){
        if(set_contains(&s_final_dict, src[i]) == SET_FALSE){
                set_add(&s_final_dict, src[i]);
            }
    }
    for(int i=0; i<dst_size; i++){
        if(set_contains(&s_final_dict, dst[i]) == SET_FALSE){
                set_add(&s_final_dict, dst[i]);
            }
    }
    char** merged = set_to_array(&s_final_dict, dict_size);
    set_destroy(&s_final_dict);
    printf("Rank %d finish merge two dict dict_size = %ld, dict: \n", my_rank, *dict_size);
    print_array(merged, *dict_size);
    return merged;
}


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

    //Build dictionary of local process
    SimpleSet set_local_dict;
    build_local_dict(num_file, my_rank, comm_sz, &set_local_dict);
    
    uint64_t dict_size;
    char** local_dict = set_to_array(&set_local_dict, &dict_size);
    set_destroy(&set_local_dict);

    //Merge dict using Reduce
    //Modified from https://gist.github.com/rmcgibbo/7178576
    const int lastpower = 1 << fastlog2(comm_sz);
    
    for(int i=lastpower; i<comm_sz; i++){
        if(my_rank == i){
            printf("Rank %d send dict_size = %ld\n", my_rank, dict_size);
            MPI_Send(&dict_size, 1, MPI_UINT64_T, i-lastpower, my_rank, MPI_COMM_WORLD);
            for(int i_d=0; i_d < dict_size; i_d++){
                int s_w = strlen(local_dict[i_d])+1;
                MPI_Send(&s_w, 1, MPI_INT, i-lastpower, my_rank+i_d+1, MPI_COMM_WORLD);
                // printf("Rank %d send word = %s\n", my_rank, local_dict[i_d]);
                MPI_Send(local_dict[i_d], s_w, MPI_CHAR, i-lastpower, my_rank+i_d+1, MPI_COMM_WORLD);
            }
        }
    }
            
    for(int i = 0; i < comm_sz-lastpower; i++){
        if(my_rank == i){
            uint64_t recv_dict_size;
            MPI_Recv(&recv_dict_size, 1, MPI_UINT64_T, i+lastpower, i+lastpower, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            char** recv_dict = malloc(recv_dict_size*sizeof(char*));
            printf("Rank %d receive dict_size = %ld\n", my_rank, recv_dict_size);
            for(int i_d = 0; i_d < recv_dict_size; i_d++){
                int s_w;
                MPI_Recv(&s_w, 1, MPI_INT, i+lastpower, i+lastpower+i_d+1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);    
                recv_dict[i_d] = malloc(s_w*sizeof(char));
                MPI_Recv(recv_dict[i_d], s_w, MPI_CHAR, i+lastpower, i+lastpower+i_d+1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                // printf("Rank %d receive word = %s\n", my_rank, recv_dict[i_d]);
            }
            uint64_t merged_dict_size;
            local_dict = merge_dict(recv_dict, recv_dict_size, local_dict, dict_size, &merged_dict_size, my_rank);
            dict_size = merged_dict_size;
            free(recv_dict);
            printf("Rank %d dict with dict_size = %ld: \n", my_rank, dict_size);
            print_array(local_dict, dict_size);
        }
    }

    for(int d=0; d < fastlog2(lastpower); d++){
        for(int k=0; k < lastpower; k += 1 << (d+1)){
            const int receiver = k;
            const int sender = k + (1 << d);
            if(my_rank == receiver){
                uint64_t recv_dict_size;
                // printf("Rank %d create receive_dict_size from rank %d tag= %d\n", my_rank, sender, sender);
                MPI_Recv(&recv_dict_size, 1, MPI_UINT64_T, sender, sender, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                printf("Rank %d receive from rank %d dict_size = %ld\n", my_rank, sender, recv_dict_size);
                char** recv_dict = malloc(recv_dict_size*sizeof(char*));
                int s_w;
                for(int i_d = 0; i_d < recv_dict_size; i_d++){
                    // printf("Rank %d create receive s_w from rank %d tag = %d\n", my_rank, sender, sender+i_d+1);
                    MPI_Recv(&s_w, 1, MPI_INT, sender, sender+i_d+1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);    
                    // printf("Rank %d receive number character = %d tag = %d\n", my_rank, s_w, sender+i_d+1);
                    recv_dict[i_d] = malloc(s_w*sizeof(char));
                    // printf("Rank %d create receive dict from rank %d tag= %d\n", my_rank, sender, sender+i_d+1);
                    MPI_Recv(recv_dict[i_d], s_w, MPI_CHAR, sender, sender+i_d+1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    // printf("Rank %d receive word = %s tag = %d\n", my_rank, recv_dict[i_d], sender+i_d+1);
                }
                uint64_t merged_dict_size;
                local_dict = merge_dict(recv_dict, recv_dict_size, local_dict, dict_size, &merged_dict_size, my_rank);
                dict_size = merged_dict_size;
                free(recv_dict);
                printf("Rank %d dict with dict_size = %ld: ", my_rank, dict_size);
                print_array(local_dict, dict_size);
            }else if(my_rank == sender){
                printf("Rank %d send dict_size = %ld, dict: ", my_rank, dict_size);
                print_array(local_dict, dict_size);
                MPI_Send(&dict_size, 1, MPI_UINT64_T, receiver, my_rank, MPI_COMM_WORLD);
                for(int i_d=0; i_d < dict_size; i_d++){
                    int s_w = strlen(local_dict[i_d])+1;
                    // printf("Rank %d send to %d number character = %d tag = %d\n", my_rank, receiver, s_w, my_rank+i_d+1);
                    MPI_Send(&s_w, 1, MPI_INT, receiver, sender+i_d+1, MPI_COMM_WORLD);
                    // printf("Rank %d send to %d word = %s tag = %d\n", my_rank, receiver, local_dict[i_d], my_rank+i_d+1);
                    MPI_Send(local_dict[i_d], s_w, MPI_CHAR, receiver, sender+i_d+1, MPI_COMM_WORLD);
                }
            }
        }
    }
    
    if(my_rank == 0){
        printf("==== Finish print array ====\n");
        print_array(local_dict, dict_size);
    }

    MPI_Finalize();
    return 0;
}