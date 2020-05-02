#include <stdio.h>
#include "mpi.h"
#include <time.h>
const int N_FILES = 50, N_LINES = 30, MAX_SIZE = 300;
char FILE_NAME[] = "Aristo-Mini-Corpus/Aristo-Mini-Corpus P-{.txt";
int np, master;

enum {INV_CORES};

void terminate(int msg)
{
    switch (msg)
    {
        case INV_CORES:
            printf("Invalid number of cores. No slaves to do the work. Please enter number of processes > 1\n");
            break;

    }
    MPI_Finalize();
    exit(1);
}


int getStart(int rank)
{
    int load = N_FILES/(np - 1);
    int ret = load * rank;
    if (rank < N_FILES%(np - 1))
        ret += rank;
    else
        ret += N_FILES%(np - 1);
    return ret;
}

int getCount(int rank)
{
    int load = N_FILES/(np - 1);
    if (rank < N_FILES%(np - 1))
        load++;
    return load;
}

// checks if sentence has the word as substring
int has_word(char* sentence, char* word)
{
    int i = 0, eni = strlen(sentence) - strlen(word) + 1;
    for (i = 0; i < eni; ++i)
    {
        int ok = 1, j = 0, enj = strlen(word);
        for (j = 0; j < enj; ++j)
        {
            if (word[j] != sentence[i + j])
            {
                ok = 0;
                break;
            }
        }
        if (ok != 0)
            return ok;
    }
    return 0;
}

// checks that the sentence has each word of the query in sentence
int has_query(char* sentence, int sentence_len, char* query)
{
    int i = 0, word_len = 0, eni = strlen(query);
    char word[MAX_SIZE];

    // tokenizing the query
    while(i < eni)
    {
        if (!isalpha(query[i]))
            i++;
        else
        {
            word_len = 0;
            while(i < eni && isalpha(query[i]))
                word[word_len++] = query[i++];
            word[word_len] = 0;
            // check if sentence contains word
            if (!has_word(sentence, word))
                return 0;
        }
    }
    return 1;
}


void get_file_name(char f_name[MAX_SIZE], int fileIdx)
{
    int i, j, k, eni, enk;
    for (i = 0, j = 0, eni = strlen(FILE_NAME); i < eni; ++i)
    {
        if (FILE_NAME[i] == '{')
        {
            char num[5];
            sprintf(num, "%d", fileIdx);
            for (k = 0, enk = strlen(num); k < enk; ++k)
                f_name[j++] = num[k];
        }
        else
            f_name[j++] = FILE_NAME[i];
    }
    f_name[j] = 0;
}

void get_results(int* n_result, char** result, char* query, int fileIdx)
{
    // get f_name from global FILE_NAME and replace last character with file idx
    char f_name[MAX_SIZE];
    FILE* file;
    int read_size; // str length of one line
    size_t buff_len = 0; // buffer length of one line
    char* sentence = NULL;

    // place the appropiate file index in the file name
    get_file_name(f_name, fileIdx + 1);

    file = fopen(f_name,"r");
    if (file == NULL)
        printf("Error opening file\n");


    while((read_size = getline(&sentence, &buff_len, file)) != -1)
    {
        if (has_query(sentence, read_size, query) != 0) // free memory?
        {
            result[*n_result] = malloc(read_size + 1);
            memcpy(result[*n_result], sentence, read_size + 1);
            (*n_result)++;
            free(sentence);
            sentence = NULL;
        }
    }
    fclose(file);
}

void work(int rank, int* n_result, char** result, char* query)
{
    int start = getStart(rank), count = getCount(rank), i;

    // get result from file i for process rank
    for (i = start; i < start + count; ++i)
        get_results(n_result, result, query, i);
}

void receive_results(int total_results, char** all_results)
{
    int i = 0, idx = 0;
    MPI_Status status;
    for (; i < np - 1; ++i)
    {
        int count;
        // number of matches
        MPI_Recv(&count, 1, MPI_INT, i, i, MPI_COMM_WORLD, &status);
        while(count--)
        {
            int len;
            // length of matching sentence + 1
            MPI_Recv(&len, 1, MPI_INT, i, i, MPI_COMM_WORLD, &status);
            all_results[idx] = malloc(len);
            MPI_Recv(all_results[idx++], len, MPI_CHAR, i, i, MPI_COMM_WORLD, &status); // matching sentence
        }
    }
}

void send_results(int rank, int n_result, char** result)
{
    int i = 0;
    // number of matches
    MPI_Send(&n_result, 1, MPI_INT, master, rank, MPI_COMM_WORLD);
    for (; i < n_result; ++i)
    {
        int len = strlen(result[i]) + 1;
        MPI_Send(&len, 1, MPI_INT, master, rank, MPI_COMM_WORLD);
        MPI_Send(result[i], len, MPI_CHAR, master, rank, MPI_COMM_WORLD);
    }
}

void write_results(int total_results, char** all_results, char* query)
{
    int i = 0;
    FILE* file = fopen("Results.txt", "w");
    fprintf(file, "Query: %s", query);
    fprintf(file, "Search Results Found = %d\n\n", total_results);

    for(i = 0; i < total_results; ++i)
        fprintf(file, "%s", all_results[i]);
}

void free2D(int n, char** arr)
{
    int i = 0;
    for (i = 0; i < n; ++i)
        free(arr[i]);
    free(arr);
}

void distribute_work_load()
{
    int i = 0;
    for (i = 0; i < np - 1; ++i)
    {
        int start = getStart(i), count = getCount(i);
        MPI_Send(&start, 1, MPI_INT, i, i, MPI_COMM_WORLD);
        MPI_Send(&count, 1, MPI_INT, i, i, MPI_COMM_WORLD);
    }
}

void receive_work_load(int rank, int* start, int* count)
{
    MPI_Status status;
    MPI_Recv(start, 1, MPI_INT, master, rank, MPI_COMM_WORLD, &status);
    MPI_Recv(count, 1, MPI_INT, master, rank, MPI_COMM_WORLD, &status);
}

int main(int argc, char* argv[])
{
    double start, end, minStart, maxEnd; // running time calculation
    int rank, n_work, n_result = 0, total_results = 0; // n_result is results found by one process, total_results is number of all results
    int startIdx, count; // starting working index and count of files to process
    char** result = NULL; // 2D char array for results foudn by 1 process
    char* query = malloc(MAX_SIZE);
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &np);

    if (np < 2)
        terminate(INV_CORES);

    // master is last process
    master = np - 1;

    if (rank == 0) // rank 0 must take input since stdin is only at rank 0
        getline(&query, &MAX_SIZE, stdin);

    // broadcast query
    MPI_Bcast(query, MAX_SIZE, MPI_CHAR, 0, MPI_COMM_WORLD);
    start = MPI_Wtime(); // for running time calculation

    if (rank == master)
        distribute_work_load();
    else
        receive_work_load(rank, &startIdx, &count);

    // get all acceptable setences in result
    if (rank != master)
    {
        result = malloc(getCount(rank) * N_LINES * sizeof(char*)); // upper bound for number of matches found in process
        work(rank, &n_result, result, query);
    }

    // reduce n_result to master in total_results
    MPI_Reduce(&n_result, &total_results, 1, MPI_INT, MPI_SUM, master, MPI_COMM_WORLD);

    // send results to master
    if (rank == master)
    {
        char** all_results = malloc(total_results * sizeof(char*)); // contains all matching sentences
        receive_results(total_results, all_results);
        write_results(total_results, all_results, query);
        free2D(total_results, all_results);
    }
    else
    {
        send_results(rank, n_result, result);
        free2D(n_result, result);
    }
    free(query);
    // calculate running time as max end - min start
    end = MPI_Wtime();
    MPI_Reduce(&start, &minStart, 1, MPI_DOUBLE, MPI_MIN, master, MPI_COMM_WORLD);
    MPI_Reduce(&end, &maxEnd, 1, MPI_DOUBLE, MPI_MAX, master, MPI_COMM_WORLD);

    if (rank == master)
        printf("Time taken: %f\n", (maxEnd - minStart));

    MPI_Finalize();
}
