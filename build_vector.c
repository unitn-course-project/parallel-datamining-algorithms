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

const char* INPUT_DATA_FOLDER = "/home/anhtu.phan/parallel-datamining-algorithms/data/";
const char* VECTOR_OUTPUT_FOLDER = "/home/anhtu.phan/parallel-datamining-algorithms/output/vector/";
const char* DICT_OUTPUT_FOLDER = "/home/anhtu.phan/parallel-datamining-algorithms/output/dict/";

// const char* INPUT_DATA_FOLDER = "/media/anhtu/046AAF9E6AAF8B4E/trento/archive/document_parses/txt_file/";
// const char* INPUT_DATA_FOLDER = "./data/";
// const char* VECTOR_OUTPUT_FOLDER = "./vector/";
// const char* DICT_OUTPUT_FOLDER = "./dict/";

const char* TITLE_EXTENSION = "_title.txt";
const char* ABSTRACT_EXTENSION = "_abstract.txt";
const char* BODY_EXTENSION = "_body.txt";
const int MAX_WORD_LEN = 50;
const int MAX_SENTENCE_LEN = 100000;
char* DOC_SEPARATION_CHAR = "$$$$$$";
const int NUM_FILE_INPUT = 100000;


void print_array(char** array, uint64_t size){
    for(int i=0; i<size; i++){
        printf("%s ", array[i]);
    }
    printf("\n");
}

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

void free_table(char** table, uint64_t row_size){
    for(int i=0; i<row_size; i++)
        free(table[i]);
    free(table);
}

SimpleSet read_stop_words(){
    FILE *fp;
    fp = fopen("./english.stop.txt", "r");
    SimpleSet stop_words;
    set_init(&stop_words);

    char ch;
    char* word = malloc(MAX_WORD_LEN* sizeof(char));
    int cnt = 0;
    while ((ch = fgetc(fp)) != EOF){
        if(ch == '\n'){
            word[cnt] = '\0';
            char* word_resized = malloc((cnt+1)*sizeof(char));
            memcpy(word_resized, word, (cnt+1)*sizeof(char));
            set_add(&stop_words, word_resized);
            cnt = 0;
            word = malloc(MAX_WORD_LEN * sizeof(char));
        }else{
            word[cnt] = ch;
            cnt++;
        }
    }
    fclose(fp);
    return stop_words;
}

void read_file(const char* file_type, int file_number, int* i_sentence_size, SimpleSet* dict, char** local_sentence, int* sentence_size, int* max_sentence_size, int* number_sentence, SimpleSet* stop_words){

    FILE *fp;
    //Get file name
    char file_index[sizeof(int)];
    sprintf(file_index, "%d", file_number);
    char buffer[strlen(INPUT_DATA_FOLDER)+sizeof(int)+strlen(file_type)];
    strcat(strcpy(buffer, INPUT_DATA_FOLDER), file_index);
    strcat(buffer, file_type);
    // printf("Read file %s\n", buffer);
    
    //Read file
    fp = fopen(buffer, "r");

    char ch;
    //store word
    char* word = malloc(MAX_WORD_LEN* sizeof(char));
    int cnt = 0;
    while ((ch = fgetc(fp)) != EOF){
        ch = tolower(ch);
        //read word by word and add to dict

        //if word has long len
        if(cnt > MAX_WORD_LEN){
            while (ch != EOF && ch != ' ' && ch != '\n')
            {
                ch = fgetc(fp);
            }
            cnt = 0;
            word = malloc(MAX_WORD_LEN*sizeof(char));
        }

        if(ch == ' ' || ch == '\n'){
            if(cnt != 0){
                word[cnt] = '\0';
                char* word_resized = realloc(word, (cnt+1)*sizeof(char));
                
                if(set_contains(stop_words, word_resized) == SET_FALSE){
                    local_sentence[(*sentence_size)] = malloc((cnt+1)*sizeof(char));
                    local_sentence[(*sentence_size)] = word_resized;
                    *sentence_size = (*sentence_size)+1;
                    (*i_sentence_size)++;
                    if(set_contains(dict, word_resized) == SET_FALSE){
                        set_add(dict, word_resized);
                    }
                }
                cnt = 0;
                word = malloc(MAX_WORD_LEN*sizeof(char));
            }
        }else{
            //Just read alpha-bet and number
            if((ch >= 48 && ch <= 57) || (ch >= 97 && ch <= 122)){
                word[cnt] = ch;
                cnt++;
            }
        }
    }
    if(cnt != 0){
        word[cnt] = '\0';
        char* word_resized = realloc(word, (cnt+1)*sizeof(char));
        if(set_contains(stop_words, word_resized) == SET_FALSE){
            local_sentence[(*sentence_size)] = malloc((cnt+1)*sizeof(char));
            local_sentence[(*sentence_size)] = word_resized;
            *sentence_size = (*sentence_size)+1;
            (*i_sentence_size)++;
            if(set_contains(dict, word_resized) == SET_FALSE){
                set_add(dict, word_resized);
            }
        }
    }
    fclose(fp);
}


void build_local_dict(int my_rank, int comm_sz, SimpleSet *dict, char** local_sentence, int* sentence_size, int* max_sentence_size, int* number_sentence){
    
    set_init(dict);
    SimpleSet stop_words = read_stop_words();

    *sentence_size = 0;
    *max_sentence_size = -1;
    *number_sentence = 0;
    
    //Just read file base on rank
    for(int i=my_rank; i<NUM_FILE_INPUT; i+=comm_sz){
        int i_sentence_size = 0;
        
        read_file(TITLE_EXTENSION, i, &i_sentence_size, dict, local_sentence, sentence_size, max_sentence_size, number_sentence, &stop_words);
        read_file(ABSTRACT_EXTENSION, i, &i_sentence_size, dict, local_sentence, sentence_size, max_sentence_size, number_sentence, &stop_words);
        // read_file(BODY_EXTENSION, i, &i_sentence_size, dict, local_sentence, sentence_size, max_sentence_size, number_sentence);
        
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
    set_destroy(&stop_words);
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
    free_table(src, src_size);
    free_table(dst, dst_size);
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
    fclose(fp);
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

    //Build dictionary and sentence of local process
    SimpleSet set_local_dict;
    set_init(&set_local_dict);

    int sentence_size;
    int local_max_sen_size;
    char** local_sentence = malloc(MAX_SENTENCE_LEN*sizeof(char*));
    int number_sentence;
    build_local_dict(my_rank, comm_sz, &set_local_dict, local_sentence, &sentence_size, &local_max_sen_size, &number_sentence);
    
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
        //Send merge dict to all process
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
        //Receive dict from master
        for(int i=1; i<comm_sz; i++){
            free_table(local_dict, dict_size);
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

    //Compute the maximum lenght of sentence to build vector
    int max_sentence_size;
    MPI_Allreduce(&local_max_sen_size, &max_sentence_size, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    
    //Build vector for each file which is read at first step
    int** doc = build_local_vector(my_rank, local_dict, dict_size, local_sentence, sentence_size, number_sentence, max_sentence_size);
    
    //Send and receive vector to store to file
    if(my_rank == 0){
        printf("========= max_sentence_size = %d =========\n", max_sentence_size);
        for(int i=0; i<number_sentence; i++){
            save_vector_2file(doc[i], max_sentence_size, i*comm_sz);
        }
        //At master receive vector from other process to save
        for(int i=1; i<comm_sz; i++){
            int num_recv_sen;
            MPI_Recv(&num_recv_sen, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for(int j=0; j<num_recv_sen; j++){
                int recv_sen[max_sentence_size];
                MPI_Recv(recv_sen, max_sentence_size, MPI_INT, i, j, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                save_vector_2file(recv_sen, max_sentence_size, i+j*comm_sz);
            }
        }
        //At master save dictionary
        FILE *fp;
        int iter_step = dict_size/comm_sz;
        printf("========= dict_size = %ld word_per_dict_file = %d =========\n", dict_size, iter_step);
        for(int i=0; i<dict_size; i++){
            if(i % iter_step == 0){
                if(i > 0){
                    fclose(fp);
                }
                char file_index[sizeof(int)];
                sprintf(file_index, "%d", i/iter_step);
                char buffer[strlen(DICT_OUTPUT_FOLDER)+sizeof(int)+strlen(".txt")+3];
                strcat(strcpy(buffer, DICT_OUTPUT_FOLDER), file_index);
                strcat(buffer, ".txt");
                fp = fopen(buffer, "w");
            }
            fprintf(fp, "%s\t%d\n", local_dict[i], i);
        }
        fclose(fp);
    }else{
        //At other process, send vector of sentence to master
        MPI_Send(&number_sentence, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        for(int i=0; i<number_sentence; i++){
            MPI_Send(doc[i], max_sentence_size, MPI_INT, 0, i, MPI_COMM_WORLD);
        }
    }
 
    MPI_Finalize();
    return 0;
}
