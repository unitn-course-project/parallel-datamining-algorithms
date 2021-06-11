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

const char* INPUT_DATA_FOLDER = "/home/anhtu.phan/parallel-datamining-algorithms/data_small/";
const char* VECTOR_OUTPUT_FOLDER = "/home/anhtu.phan/parallel-datamining-algorithms/output/vector/";
const char* DICT_OUTPUT_FOLDER = "/home/anhtu.phan/parallel-datamining-algorithms/output/dict/";

// const char* INPUT_DATA_FOLDER = "./data/";
// const char* VECTOR_OUTPUT_FOLDER = "./data/";
// const char* DICT_OUTPUT_FOLDER = "./dict/";

const char* TITLE_EXTENSION = "_title.txt";
const int MAX_WORD_LEN = 50;
const int MAX_SENTENCE_LEN = 1000;
char* DOC_SEPARATION_CHAR = "#$#$";


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
            printf("Read file %s\n", entry->d_name);
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

void build_local_dict(int num_file, int my_rank, int comm_sz, SimpleSet *dict, char** local_sentence, int* sentence_size, int* max_sentence_size, int* number_sentence){
    FILE *fp;
    set_init(dict);
    
    *sentence_size = 0;
    *max_sentence_size = -1;
    *number_sentence = 0;
    
    //Just read file base on rank
    for(int i=my_rank; i<num_file; i+=comm_sz){
        if(i == (i + comm_sz*2))
            break;
        //Get file name
        char file_index[sizeof(int)];
        sprintf(file_index, "%d", i);
        char buffer[strlen(INPUT_DATA_FOLDER)+sizeof(int)+strlen(TITLE_EXTENSION)];
        strcat(strcpy(buffer, INPUT_DATA_FOLDER), file_index);
        strcat(buffer, TITLE_EXTENSION);

        //Read file
        fp = fopen(buffer, "r");

        char ch;
        //store word
        char* word = malloc(MAX_WORD_LEN* sizeof(char));
        int cnt = 0;
        int i_sentence_size = 0;
        while ((ch = fgetc(fp)) != EOF){
            ch = tolower(ch);
            //read word by word and add to dict
            if(ch == ' ' || ch == '\n'){
                if(cnt != 0){
                    word[cnt] = '\0';
                    char* word_resized = realloc(word, (cnt+1)*sizeof(char));
                    local_sentence[(*sentence_size)] = malloc((cnt+1)*sizeof(char));
                    local_sentence[(*sentence_size)] = word_resized;
                    *sentence_size = (*sentence_size)+1;
                    i_sentence_size++;
                    if(set_contains(dict, word_resized) == SET_FALSE){
                        set_add(dict, word_resized);
                    }
                    cnt = 0;
                    word = malloc(MAX_WORD_LEN*sizeof(char));
                }
            }else{
                word[cnt] = ch;
                cnt++;
            }
        }
        if(cnt != 0){
            word[cnt] = '\0';
            char* word_resized = realloc(word, (cnt+1)*sizeof(char));
            local_sentence[(*sentence_size)] = malloc((cnt+1)*sizeof(char));
            local_sentence[(*sentence_size)] = word_resized;
            *sentence_size = (*sentence_size)+1;
            i_sentence_size++;
            if(set_contains(dict, word_resized) == SET_FALSE){
                set_add(dict, word_resized);
            }
        }

        if(i_sentence_size > 0){
            local_sentence[*sentence_size] = DOC_SEPARATION_CHAR;
            *sentence_size = (*sentence_size) + 1;
            *number_sentence = (*number_sentence) + 1;
        }
        if(i_sentence_size > *max_sentence_size){
            *max_sentence_size = i_sentence_size;
        }
    }
    local_sentence = realloc(local_sentence, (*sentence_size)*(sizeof(char*)));
}


char** merge_dict(char** src, uint64_t src_size, char** dst, uint64_t dst_size, uint64_t* dict_size, int my_rank){
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
    return merged;
}


void save_vector_2file(int* vector, int vector_size, int doc_index){
    char file_index[sizeof(int)];
    sprintf(file_index, "%d", doc_index);
    char buffer[strlen(VECTOR_OUTPUT_FOLDER)+sizeof(int)+strlen(".data")];
    strcat(strcpy(buffer, VECTOR_OUTPUT_FOLDER), file_index);
    strcat(buffer, ".data");
    FILE *fp;
    fp = fopen(buffer, "wb");
    fwrite(vector, sizeof(int), vector_size, fp);
}


int** build_local_vector(int rank, char** local_dict, int dict_size, char** sentence, int sentence_size, int number_sentence, int max_sen_size){
    int** v_setence = malloc(number_sentence*sizeof(int*));
    int i_w = 0;
    int i_s = 0;
    for(int i=0; i<number_sentence; i++){
        v_setence[i] = malloc(max_sen_size*sizeof(int));
        for(int j=0; j<max_sen_size; j++){
            v_setence[i][j] = -1;
        }
    }
    
    for(int i=0; i<sentence_size-1; i++){
        char* word = sentence[i];
        if(strcmp(word, DOC_SEPARATION_CHAR) == 0){
            i_w = 0;
            i_s++;
            continue;
        }
        for(int j=0; j<dict_size; j++){
            if(strcmp(word, local_dict[j]) == 0){
                v_setence[i_s][i_w] = j;
                break;
            }
        }
        i_w++;
    }
    return v_setence;
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


    //Build dictionary and sentence of local process
    SimpleSet set_local_dict;
    
    int sentence_size;
    int local_max_sen_size;
    char** local_sentence = malloc(MAX_SENTENCE_LEN * sizeof(char*));
    int number_sentence;
    build_local_dict(num_file, my_rank, comm_sz, &set_local_dict, local_sentence, &sentence_size, &local_max_sen_size, &number_sentence);
    
    uint64_t dict_size;
    char** local_dict = set_to_array(&set_local_dict, &dict_size);
    set_destroy(&set_local_dict);


    //Merge dict using Reduce
    //Modified from https://gist.github.com/rmcgibbo/7178576
    const int lastpower = 1 << fastlog2(comm_sz);
    
    //If number of process is not power of 2
    //each of the ranks greater than the last power of 2 less than comm_size
    //send data to first processes
    for(int i=lastpower; i<comm_sz; i++){
        if(my_rank == i){
            MPI_Send(&dict_size, 1, MPI_UINT64_T, i-lastpower, my_rank, MPI_COMM_WORLD);
            for(int i_d=0; i_d < dict_size; i_d++){
                int s_w = strlen(local_dict[i_d])+1;
                MPI_Send(&s_w, 1, MPI_INT, i-lastpower, my_rank+i_d+1, MPI_COMM_WORLD);
                MPI_Send(local_dict[i_d], s_w, MPI_CHAR, i-lastpower, my_rank+i_d+1, MPI_COMM_WORLD);
            }
        }
    }
    
    //first processes receive data from end processes (greater than last power of 2)
    for(int i = 0; i < comm_sz-lastpower; i++){
        if(my_rank == i){
            uint64_t recv_dict_size;
            MPI_Recv(&recv_dict_size, 1, MPI_UINT64_T, i+lastpower, i+lastpower, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            char** recv_dict = malloc(recv_dict_size*sizeof(char*));
            for(int i_d = 0; i_d < recv_dict_size; i_d++){
                int s_w;
                MPI_Recv(&s_w, 1, MPI_INT, i+lastpower, i+lastpower+i_d+1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);    
                recv_dict[i_d] = malloc(s_w*sizeof(char));
                MPI_Recv(recv_dict[i_d], s_w, MPI_CHAR, i+lastpower, i+lastpower+i_d+1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }
            uint64_t merged_dict_size;
            local_dict = merge_dict(recv_dict, recv_dict_size, local_dict, dict_size, &merged_dict_size, my_rank);
            dict_size = merged_dict_size;
            free(recv_dict);
        }
    }


    //For process smaller than last power of 2
    //Create a tree with different level of send and receive
    for(int d=0; d < fastlog2(lastpower); d++){
        for(int k=0; k < lastpower; k += 1 << (d+1)){
            const int receiver = k;
            const int sender = k + (1 << d);
            if(my_rank == receiver){
                uint64_t recv_dict_size;
                MPI_Recv(&recv_dict_size, 1, MPI_UINT64_T, sender, sender, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                char** recv_dict = malloc(recv_dict_size*sizeof(char*));
                int s_w;
                for(int i_d = 0; i_d < recv_dict_size; i_d++){
                    MPI_Recv(&s_w, 1, MPI_INT, sender, sender+i_d+1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);    
                    recv_dict[i_d] = malloc(s_w*sizeof(char));
                    MPI_Recv(recv_dict[i_d], s_w, MPI_CHAR, sender, sender+i_d+1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    
                }
                uint64_t merged_dict_size;
                local_dict = merge_dict(recv_dict, recv_dict_size, local_dict, dict_size, &merged_dict_size, my_rank);
                dict_size = merged_dict_size;
                free(recv_dict);
            }else if(my_rank == sender){
                MPI_Send(&dict_size, 1, MPI_UINT64_T, receiver, my_rank, MPI_COMM_WORLD);
                for(int i_d=0; i_d < dict_size; i_d++){
                    int s_w = strlen(local_dict[i_d])+1;
                    MPI_Send(&s_w, 1, MPI_INT, receiver, sender+i_d+1, MPI_COMM_WORLD);
                    MPI_Send(local_dict[i_d], s_w, MPI_CHAR, receiver, sender+i_d+1, MPI_COMM_WORLD);
                }
            }
        }
    }
    

    //Broadcast result to all process
    if(my_rank == 0){
        // print_array(local_dict, dict_size);
        for(int i=1; i<comm_sz; i++){
            MPI_Bcast(&dict_size, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
            for(int i_d=0; i_d < dict_size; i_d++){
                int s_w = strlen(local_dict[i_d])+1;
                MPI_Bcast(&s_w, 1, MPI_INT, 0, MPI_COMM_WORLD);
                MPI_Bcast(local_dict[i_d], s_w, MPI_CHAR, 0, MPI_COMM_WORLD);
            }
        }
    }
    else{
        for(int i=1; i<comm_sz; i++){
            MPI_Bcast(&dict_size, 1, MPI_UINT64_T, 0, MPI_COMM_WORLD);
            local_dict = malloc(dict_size*sizeof(char*));
            int s_w;
            for(int i_d = 0; i_d < dict_size; i_d++){
                MPI_Bcast(&s_w, 1, MPI_INT, 0, MPI_COMM_WORLD);
                local_dict[i_d] = malloc(s_w*sizeof(char));
                MPI_Bcast(local_dict[i_d], s_w, MPI_CHAR, 0, MPI_COMM_WORLD);
            }
        }
    }
    int max_sentence_size;
    MPI_Allreduce(&local_max_sen_size, &max_sentence_size, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    
    int** doc = build_local_vector(my_rank, local_dict, dict_size, local_sentence, sentence_size, number_sentence, max_sentence_size);
    
    //Receive vector to store to file
    if(my_rank == 0){
        printf("========= max_sentence_size = %d =========\n", max_sentence_size);
        for(int i=0; i<number_sentence; i++){
            save_vector_2file(doc[i], max_sentence_size, i);
        }
        for(int i=1; i<comm_sz; i++){
            int num_recv_sen;
            MPI_Recv(&num_recv_sen, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for(int j=0; j<num_recv_sen; j++){
                int recv_sen[max_sentence_size];
                MPI_Recv(recv_sen, max_sentence_size, MPI_INT, i, j, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                save_vector_2file(recv_sen, max_sentence_size, i+j);
            }
        }
        //Save dictionary
        FILE *fp;
        int iter_step = dict_size/comm_sz;
        for(int i=0; i<dict_size; i++){
            if(i % iter_step == 0){
                if(i > 0){
                    fclose(fp);
                }
                char file_index[sizeof(int)];
                sprintf(file_index, "%d", i/iter_step);
                char buffer[strlen(DICT_OUTPUT_FOLDER)+sizeof(int)+strlen(".txt")];
                strcat(strcpy(buffer, DICT_OUTPUT_FOLDER), file_index);
                strcat(buffer, ".txt");
                fp = fopen(buffer, "w");
            }
            fprintf(fp, "%s\t%d\n", local_dict[i], i);
        }
    }else{
        MPI_Send(&number_sentence, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        for(int i=0; i<number_sentence; i++){
            MPI_Send(doc[i], max_sentence_size, MPI_INT, 0, i, MPI_COMM_WORLD);
        }
    }
    MPI_Finalize();
    return 0;
}