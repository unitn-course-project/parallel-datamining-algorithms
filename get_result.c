#include "set.c"
#include <stdio.h>
#include <inttypes.h>
#include<stdlib.h>


void print_set(SimpleSet set){
    uint64_t set_size;
    char** array = set_to_array(&set, &set_size);
    printf("set size = %" PRIu64 "\n", set_size);
    for(int i=0; i < set_size; i++){
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

int main(int argc, char** argv) {
    // SimpleSet set;
    // set_init(&set);
    // set_add(&set, "orange");
    // set_add(&set, "blue");
    // set_add(&set, "red");
    // set_add(&set, "green");
    // set_add(&set, "yellow");
    // set_add(&set, "orange");
    // print_set(set);
    // if (set_contains(&set, "yellow") == SET_TRUE) {
    //     printf("Set contains 'yellow'!\n");
    // } else {
    //     printf("Set does not contains 'yellow'!\n");
    // }

    // if (set_contains(&set, "purple") == SET_TRUE) {
    //     printf("Set contains 'purple'!\n");
    // } else {
    //     printf("Set does not contains 'purple'!\n");
    // }

    // set_destroy(&set);
    // printf(" 1 << %d = %d\n", 8, (1<<8));
    // printf("fastlog2(%d) = %d \n 1 << fastlog2(%d) = %d\n", 1025, fastlog2(1025), 1025, (1 << fastlog2(1025)));
    FILE *fp_cluster;
    fp_cluster = fopen("./mpi_kmean-4.bin", "rb");
    int cluster[1000];
    fread(cluster, sizeof(int), 1000, fp_cluster);
    fclose(fp_cluster);
    
    FILE *fp_result;
    fp_result = fopen("./result_4.csv", "w");
    fprintf(fp_result, "%s\t%s\n", "cluster", "doc");
    for(int i_f=0; i_f<1000; i_f++){
      fprintf(fp_result, "%d\t", cluster[i_f]);
      FILE *fp;
      char file_index[sizeof(int)];
      sprintf(file_index, "%d", i_f);
      char buffer[strlen("/home/anhtu/Project/trento/parallel-datamining-algorithms/vector/")+sizeof(int)+strlen(".data")];
      strcat(strcpy(buffer, "/home/anhtu/Project/trento/parallel-datamining-algorithms/vector/"), file_index);
      strcat(buffer, ".data");
      fp = fopen(buffer, "rb");
      int sentence[300];
      fread(sentence, sizeof(int), 300, fp);
      for(int i=0; i<300; i++){
        int word_id = sentence[i];
        // printf("Word id = %d\n", word_id);
        if(word_id == -1)
          break;
        int dict_file = word_id/114;
        FILE *fp_dict;
        char file_dict_index[sizeof(int)];
        sprintf(file_dict_index, "%d", dict_file);
        char dict_file_name[strlen("/home/anhtu/Project/trento/parallel-datamining-algorithms/dict/")+sizeof(int)+strlen(".txt")];
        strcat(strcpy(dict_file_name, "/home/anhtu/Project/trento/parallel-datamining-algorithms/dict/"), file_dict_index);
        strcat(dict_file_name, ".txt");
        // printf("Read dict_file %s\n", dict_file_name);
        fp_dict = fopen(dict_file_name, "r");
        char* word_;
        char* word = malloc(50*sizeof(char));
        char ch;
        int cnt = 0;
        while((ch = fgetc(fp_dict)) != EOF){
          if(ch == '\t'){
            word[cnt] ='\0';
            word_ = malloc((cnt+1)*sizeof(char));
            memcpy(word_, word, (cnt+1)*sizeof(char));
            // printf("Word = %s\n", word_);
            cnt = 0;
            word = malloc(50*sizeof(char));
          }else if(ch == '\n'){
            word[cnt] = '\0';
            int word_index = atoi(word);
            // printf("Word index = %d\n", word_index);
            if(word_index == word_id){
              fprintf(fp_result, "%s ", word_);
            }
            cnt = 0;
            word = malloc(50*sizeof(char));
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