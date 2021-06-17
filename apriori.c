#include<stdio.h>
#include<mpi.h>
#include <stdlib.h>
#include<string.h>
#include<unistd.h>
#include "set.c"

char* DICT_INPUT_PATH = "/home/anhtu.phan/parallel-datamining-algorithms/output/dict/";
char* VECTOR_INPUT_PATH = "/home/anhtu.phan/parallel-datamining-algorithms/output/cluster/";

int SENTENCE_SIZE = 339;
int DICT_SIZE = 371;
int DICT_WORD_PER_FILE = 92;
const int MAX_WORD_LEN = 50;
int SUPPORT_THRES = 1;
int MAX_NUMBER_BASKET_SIZE = 5;
int NUM_FILE_INPUT = 5;
int CLUSTER = 0;


char* build_file_name(char* folder_input, int file_index, char* extension){
    char i_c[sizeof(int)];
    sprintf(i_c, "%d", file_index);
    char* buffer = malloc((strlen(folder_input)+sizeof(int)+strlen(extension))*sizeof(char));
    strcat(strcpy(buffer, folder_input), i_c);
    strcat(buffer, extension);
    return buffer;
}

void read_file(int my_rank, int comm_sz, char* input_path, int** sentence){
    FILE *fp;
    for(int i=my_rank; i<NUM_FILE_INPUT; i+=comm_sz){
        char* buffer = build_file_name(input_path, i, ".data");
        if(access(buffer, F_OK) == 0){
            sentence[i/comm_sz] = malloc(SENTENCE_SIZE * sizeof(int));
            fp = fopen(buffer, "rb");
            fread(sentence[i/comm_sz], sizeof(int), SENTENCE_SIZE, fp);
        }else{
            sentence[i/comm_sz] = NULL;
        }
        free(buffer);
    }
    fclose(fp);
}

int fact(int n){
    int s = 1;
    for(int i=2; i<=n; i++){
        s *= i;
    }
    return s;
}

void free_table(int** arr, int row){
    for(int i=0; i<row; i++)
        free(arr[i]);
    free(arr);
}

void print_array(int** arr, int row, int col){
    for(int i=0; i<row; i++){
        for(int j=0; j<col; j++){
            printf("%d ", arr[i][j]);
        }
        printf("\n");
    }
}



int* merge_array(int* src, int* dst, int size){
    int* result = malloc((size+1)*sizeof(int));
    for(int i=0; i<size; i++){
        result[i] = src[i];
    }
    for(int i=0; i<size; i++){
        int check = 0;
        for(int j=0; j<size; j++){
            if (dst[i] == result[i])
                check++;
        }
        if(check != size){
            result[size] = dst[i];
            break;
        }
    }
    return result;
}


char* get_word_from_dict(int word_id){
    int dict_file = word_id/DICT_WORD_PER_FILE;
    FILE *fp_dict;

    char* dict_file_name = build_file_name(DICT_INPUT_PATH, dict_file, ".txt");
    fp_dict = fopen(dict_file_name, "r");
    free(dict_file_name);
    
    char* word_;
    char* word = malloc(MAX_WORD_LEN*sizeof(char));
    char ch;
    int cnt = 0;
    while((ch = fgetc(fp_dict)) != EOF){
        if(ch == '\t'){
            word[cnt] ='\0';
            word_ = malloc((cnt+1)*sizeof(char));
            memcpy(word_, word, (cnt+1)*sizeof(char));
            // printf("Word = %s\n", word_);
            cnt = 0;
            word = malloc(MAX_WORD_LEN*sizeof(char));
        }else if(ch == '\n'){
            word[cnt] = '\0';
            int word_index = atoi(word);
            // printf("Word index = %d\n", word_index);
            if(word_index == word_id){
                fclose(fp_dict);
                return word_;
            }
            cnt = 0;
            word = malloc(MAX_WORD_LEN*sizeof(char));
        }else{
            word[cnt] = ch;
            cnt++;
        }
    }
    fclose(fp_dict);
    return NULL;
}

void construct_param(int argc, char **argv){

    for(int i=1; i<argc; ++i){
        char* arg = argv[i];
        if(strcmp(arg, "-dictInputFolder") == 0 || strcmp(arg, "--dictInputFolder") == 0){
            DICT_INPUT_PATH = argv[i+1];
        }else if(strcmp(arg, "-vectorInputFolder") == 0 || strcmp(arg, "--vectorInputFolder") == 0){
            VECTOR_INPUT_PATH = argv[i+1];
        }else if(strcmp(arg, "-sentenceSize") == 0 || strcmp(arg, "--sentenceSize") == 0){
            SENTENCE_SIZE = atoi(argv[i+1]);
        }else if(strcmp(arg, "-dictSize") == 0 || strcmp(arg, "--dictSize") == 0){
            DICT_SIZE = atoi(argv[i+1]);
        }else if(strcmp(arg, "-dictWordPerFile") == 0 || strcmp(arg, "--dictWordPerFile") == 0){
            DICT_WORD_PER_FILE = atoi(argv[i+1]);
        }else if(strcmp(arg, "-supportThres") == 0 || strcmp(arg, "--supportThres") == 0){
            SUPPORT_THRES = atoi(argv[i+1]);
        }else if(strcmp(arg, "-maxNumberBasketSize") == 0 || strcmp(arg, "--maxNumberBasketSize") == 0){
            MAX_NUMBER_BASKET_SIZE = atoi(argv[i+1]);
        }else if(strcmp(arg, "-numFileInput") == 0 || strcmp(arg, "--numFileInput") == 0){
            NUM_FILE_INPUT = atoi(argv[i+1]);
        }else if(strcmp(arg, "-clusterId") == 0 || strcmp(arg, "--clusterId") == 0){
            CLUSTER = atoi(argv[i+1]);
        }
    }
}

int main(int argc, char **argv){
    construct_param(argc, argv);
    
    int my_rank, comm_sz;
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);

    int num_sentence = ((NUM_FILE_INPUT-1-my_rank)/comm_sz+1);
    int** sentence = malloc(num_sentence*sizeof(int*));

    char* buffer = build_file_name(VECTOR_INPUT_PATH, CLUSTER, "/");
    read_file(my_rank, comm_sz, buffer, sentence);

    int* local_support = malloc(DICT_SIZE*sizeof(int));
    for(int i=0; i<DICT_SIZE; i++){
        local_support[i] = 0;
    }
    
    //Count 1-item
    for(int i=0; i<num_sentence; i++){
        if(sentence[i] != NULL){
            SimpleSet words;
            set_init(&words);
            for(int j=0; j<SENTENCE_SIZE; j++){
                if(sentence[i][j] == -1)
                    break;
                char word_index[sizeof(int)];
                sprintf(word_index, "%d", sentence[i][j]);
                if(set_contains(&words, word_index) == SET_FALSE){
                    set_add(&words, word_index);
                    local_support[sentence[i][j]] += 1;
                }
            }
            set_destroy(&words);
        }
    }

    int* support = malloc(DICT_SIZE*sizeof(int));
    MPI_Allreduce(local_support, support, DICT_SIZE, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    int number_input = DICT_SIZE;
    int** prev_items = malloc(DICT_SIZE*sizeof(int*));
    for(int i=0; i<DICT_SIZE; i++){
        prev_items[i] = malloc(1*sizeof(int));
        *prev_items[i] = i;
    }

    FILE *fp_result;	
    if(my_rank == 0){
        char* result_file_name = build_file_name("./apriori-cluster-", CLUSTER, ".txt");
        fp_result = fopen(result_file_name, "w");
    }
    
    for(int basket_size=2; basket_size<=MAX_NUMBER_BASKET_SIZE; basket_size++){
        if(number_input == 0)
            break;

        if(my_rank == 0){
            fprintf(fp_result, "========= Frequent %d word =========\n", basket_size-1);
        }

        //Filter L(k-1) candidate 
        int** candidate = malloc(number_input*sizeof(int*));
        int number_candidate = 0;
        for(int i=0; i<number_input; i++){       
            if(support[i] >= SUPPORT_THRES){
                candidate[number_candidate] = malloc((basket_size-1)*sizeof(int));
                memcpy(candidate[number_candidate], prev_items[i], (basket_size-1)*sizeof(int));
                
                //Write result
                if(my_rank == 0){
                    for(int i_p=0; i_p<(basket_size-1); i_p++){
                        char* word = get_word_from_dict(prev_items[i][i_p]);
                        fprintf(fp_result, "%s ", word);
                    }
                    fprintf(fp_result, "-> %d\n", support[i]);
                }
                number_candidate++;
            }
        }
        
        if(number_candidate < 2)
            break;

        candidate = realloc(candidate, number_candidate*sizeof(int*));
        free_table(prev_items, number_input); 

        //Construct L(k) candidate
        int number_next_candidate = (number_candidate*(number_candidate-1))/2;
        int** next_candidate = malloc(number_next_candidate*sizeof(int*));
        int i_next = 0;
        for(int i=0; i<number_candidate-1; i++){
            for(int j=i+1; j<number_candidate; j++){
                next_candidate[i_next] = malloc(basket_size*sizeof(int));
                next_candidate[i_next] = merge_array(candidate[i], candidate[j], basket_size-1);
                i_next++;
            }
        }

        //Count support
        free(local_support);
        local_support = malloc(number_next_candidate*sizeof(int));
        for(int i=0; i<number_next_candidate; i++){
            local_support[i] = 0;
        }

        //Count k-item
        for(int i_c=0; i_c<number_next_candidate; i_c++){
            for(int i=0; i<num_sentence; i++){
                if(sentence[i] != NULL){
                    int check = 0;
                    for(int i_t=0; i_t<basket_size; i_t++){
                        for(int j=0; j<SENTENCE_SIZE; j++){
                            if(sentence[i][j] == -1)
                                break;  
                            if(sentence[i][j] == next_candidate[i_c][i_t]){
                                check++;
                                break;
                            }
                        }
                    }
                    if(check == basket_size)
                        local_support[i_c] += 1;
                }
            }
        }
    
        free(support);
        support = malloc(number_next_candidate*sizeof(int));
        MPI_Allreduce(local_support, support, number_next_candidate, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
        
        prev_items = next_candidate;
        number_input = number_next_candidate;
    }

    if(my_rank==0){
        fclose(fp_result);
    }
    MPI_Finalize();
    return 0;
}