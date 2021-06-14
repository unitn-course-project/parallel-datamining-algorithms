#include <stdio.h>
#include <inttypes.h>
#include<stdlib.h>
#include<string.h>

const int SENTENCE_SIZE = 3443;
const int DICT_WORD_PER_FILE = 4086;
const int MAX_WORD_LEN = 500;

int main(int argc, char** argv) {
    // FILE *fp_cluster;
    // fp_cluster = fopen("./mpi_kmean-4.bin", "rb");
    // int cluster[1000];
    // fread(cluster, sizeof(int), 1000, fp_cluster);
    // fclose(fp_cluster);
    
    // FILE *fp_cluster;
    // fp_cluster = fopen("./mpi_kmean-4.bin", "rb");
    // int cluster[1000];
    // fread(cluster, sizeof(int), 1000, fp_cluster);
    // fclose(fp_cluster);
    
    
    FILE *fp_result;
    fp_result = fopen("./result_test.txt", "w");
    fprintf(fp_result, "%s\n", "doc");
    for(int i_f=0; i_f<7; i_f++){
      // fprintf(fp_result, "%d\t", cluster[i_f]);
      FILE *fp;
      char file_index[sizeof(int)];
      sprintf(file_index, "%d", i_f);
      char buffer[strlen("/home/anhtu/Project/trento/parallel-datamining-algorithms/vector/")+sizeof(int)+strlen(".data")];
      strcat(strcpy(buffer, "/home/anhtu/Project/trento/parallel-datamining-algorithms/vector/"), file_index);
      strcat(buffer, ".data");
      fp = fopen(buffer, "rb");
      int sentence[SENTENCE_SIZE];
      fread(sentence, sizeof(int), SENTENCE_SIZE, fp);
      for(int i=0; i<SENTENCE_SIZE; i++){
        int word_id = sentence[i];
        // printf("Word id = %d\n", word_id);
        if(word_id == -1)
          break;
        int dict_file = word_id/DICT_WORD_PER_FILE;
        FILE *fp_dict;
        char file_dict_index[sizeof(int)];
        sprintf(file_dict_index, "%d", dict_file);
        char dict_file_name[strlen("/home/anhtu/Project/trento/parallel-datamining-algorithms/dict/")+sizeof(int)+strlen(".txt")];
        strcat(strcpy(dict_file_name, "/home/anhtu/Project/trento/parallel-datamining-algorithms/dict/"), file_dict_index);
        strcat(dict_file_name, ".txt");
        // printf("Read dict_file %s\n", dict_file_name);
        fp_dict = fopen(dict_file_name, "r");
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
              fprintf(fp_result, "%s ", word_);
            }
            cnt = 0;
            word = malloc(MAX_WORD_LEN*sizeof(char));
          }else{
            word[cnt] = ch;
            cnt++;
          }
        }

        fclose(fp_dict);
      }
      
      fprintf(fp_result, "\n");
      fclose(fp);
    }
    fclose(fp_result);
}