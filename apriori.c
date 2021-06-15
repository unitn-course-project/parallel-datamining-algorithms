#include<stdio.h>
#include<mpi.h>
#include <stdlib.h>
#include<string.h>
#include<unistd.h>
#include "set.c"

const char* VECTOR_INPUT_PATH = "./cluster/";
const int SENTENCE_SIZE = 406;
const int DICT_SIZE = 478;
const int SUPPORT_THRES = 2;

void read_file(int my_rank, int num_file, int comm_sz, char* input_path, int** sentence){
    FILE *fp;
    for(int i=my_rank; i<num_file; i+=comm_sz){
        char i_f[sizeof(int)];
        sprintf(i_f, "%d", i);
        char buffer[strlen(input_path)+sizeof(int)+strlen(".data")];
        strcat(strcpy(buffer, input_path), i_f);
        strcat(buffer, ".data");
    
        if(access(buffer, F_OK) == 0){
            sentence[i/comm_sz] = malloc(SENTENCE_SIZE * sizeof(int));
            fp = fopen(buffer, "rb");
            fread(sentence[i/comm_sz], sizeof(int), SENTENCE_SIZE, fp);
        }else{
            sentence[i/comm_sz] = NULL;
        }
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
    printf("Merge [");
    for(int i=0; i<size; i++){
        printf("%d ", src[i]);
        result[i] = src[i];
    }
    printf("] with [");
    for(int i=0; i<size; i++){
        printf("%d ", dst[i]);
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
    printf("] to [");
    for(int i=0; i<size+1; i++){
        printf("%d ", result[i]);
    }
    printf("]\n");
    return result;
}

int main(void){
    int my_rank, comm_sz;
    MPI_Init(NULL, NULL);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &comm_sz);

    int num_file = 8;
    int cluster = 0;
    MPI_Bcast(&num_file, 1, MPI_INT, 0, MPI_COMM_WORLD);
    
    int num_sentence = ((num_file-1-my_rank)/comm_sz+1);
    int** sentence = malloc(num_sentence*sizeof(int*));

    char i_c[sizeof(int)];
    sprintf(i_c, "%d", cluster);
    char buffer[strlen(VECTOR_INPUT_PATH)+sizeof(int)+strlen("/")];
    strcat(strcpy(buffer, VECTOR_INPUT_PATH), i_c);
    strcat(buffer, "/");
    read_file(my_rank, num_file, comm_sz, buffer, sentence);

    int local_support[DICT_SIZE];
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

    int support[DICT_SIZE];
    MPI_Allreduce(local_support, support, DICT_SIZE, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    int number_input = DICT_SIZE;
    int** input_candidate = malloc(DICT_SIZE*sizeof(int*));
    for(int i=0; i<DICT_SIZE; i++){
        input_candidate[i] = malloc(1*sizeof(int));
        *input_candidate[i] = i;
    }

    for(int basket_size=2; basket_size<3; basket_size++){
        //Construct candidate
        int** candidate = malloc(number_input*sizeof(int*));
        int number_candidate = 0;
        for(int i=0; i<number_input; i++){
            if(support[i] > SUPPORT_THRES){
                candidate[number_candidate] = malloc((basket_size-1)*sizeof(int));
                memcpy(candidate[number_candidate], input_candidate[i], (basket_size-1)*sizeof(int));
                number_candidate++;
            }
        }
        candidate = realloc(candidate, number_candidate*sizeof(int*));
        free_table(input_candidate, number_input);
        if(my_rank == 0){
            printf("===== CANDIDATE with size =%d =====\n", number_candidate);
            print_array(candidate, number_candidate, basket_size-1);
        }
        
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
        if(my_rank == 0){
            printf("===== NEXT CANDIDATE with size =%d =====\n", number_next_candidate);
            print_array(next_candidate, number_next_candidate, basket_size);
        }
    }

    MPI_Finalize();
    return 0;
}