# High Performace Computing for Data Science
## This is a project for High Performace Computing for Data Science course at University of Trento
The covid-19 outbreak in over the world increasingly become the most important research topic in last year. A huge amount of articles and publications have been published about many different angles of covid-19. It is paramount important for scientists to keep up with new information on the virus, but the most obstacle is a large number of articles that can not easily process by humans. It is necessary to using data mining algorithms to process this data. However, with this amount of data (GB), some normal serial text mining algorithms still need a lot of time to process.

Therefore, in our project, we will use the High Performace Computing (HPC) cluster to execute some text-mining-parallel algorithms in order to extract knowledge from a big data set of articles. We will cluster similar articles into groups. Then, over articles in each group, we will finding the set of words that frequently appear together in the most of articles.

See [report.pdf](report.pdf) for more detail.

## Structure of code
- [build_vector.c](build_vector.c): Preprocess, build dictionary and vectorize text input task
- [mpi_kmean.cpp](mpi_kmean.cpp): Clustering task
- [apriori.c](apriori.c): Finding frequent words task

