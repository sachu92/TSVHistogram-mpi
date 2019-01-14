#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mpi.h"
#include "tsv_helper.h"

#define BYTESINMB 1048576

int main (int argc, char *argv[])
{
    int i;
    int mpi_rank, mpi_num_procs;
    int column;
    int num_bins;
    int local_error_flag = 0;
    int global_error_flag = 0;
    int file_read_done_flag = 0;
    int first_read_from_file_flag = 1;
    int num_cols_in_line = 0;

    FILE * data_file = NULL;

    size_t file_read_result;
    size_t file_size;
    size_t buffer_size;
    size_t chunk_size;
    size_t current_pos_in_file;

    long chunk_processed_line_count;
    long buffer_processed_line_count = 0;
    long data_processed_line_count = 0;
    long chunk_discarded_line_count;
    long buffer_discarded_line_count = 0;
    long data_discarded_line_count = 0;
    long *chunk_bins = NULL;
    long *buffer_bins = NULL;
    long *data_bins = NULL;

    char *buffer = NULL;
    char *chunk = NULL;
    char error_message[100];
    char file_size_string[100];
    char num_cols_buffer[256];

    double data_min = 1e42, data_max = -1e42;
    double bin_width;

    // Initialize MPI environment
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_num_procs);

    chunk_size = 50*BYTESINMB;
    buffer_size = mpi_num_procs*chunk_size;
    if(mpi_rank == 0) // root node
    {
        if(argc < 6)
        {
            global_error_flag = 1;
            sprintf(error_message, "Usage: %s <column> <min-value> <max-value> <num-bins> <file>\n", argv[0]);
        }
        if(!global_error_flag)
        {
            column = atof(argv[1]);
            data_min = atof(argv[2]);
            data_max = atof(argv[3]);
            num_bins = atof(argv[4]);
            data_file = fopen(argv[5], "r");
            if (data_file == NULL)
            {
                global_error_flag = 1;
                strcpy(error_message, "Cannot open file.");
            }
        }
        if(!global_error_flag)
        {
            // obtain file size:
            fseek (data_file , 0 , SEEK_END);
            file_size = ftell (data_file);
            rewind (data_file);
            getSizeInHumanReadableForm(file_size,file_size_string);

            buffer = (char*) malloc(sizeof(char)*buffer_size);
            if(buffer == NULL)
            {
                global_error_flag = 1;
                strcpy(error_message, "Cannot allocate memory for buffer.");
            }
        }
        if(!global_error_flag)
        {
            data_bins = (long*) malloc(sizeof(long)*num_bins);
            buffer_bins = (long*) malloc(sizeof(long)*num_bins);
            if(data_bins == NULL || buffer_bins == NULL)
            {
                global_error_flag = 1;
                strcpy(error_message, "Cannot allocate memory for bins.");
            }
            else
            {
                for(i = 0;i < num_bins;i++)
                    data_bins[i] = 0;
            }
        }
    }

    MPI_Bcast(&global_error_flag, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if(!global_error_flag)
    {
        MPI_Bcast(&file_size, 1, MPI_LONG, 0, MPI_COMM_WORLD);
        chunk = (char *) malloc(sizeof(char)*chunk_size);
        chunk_bins = (long *)malloc(sizeof(long)*num_bins);
        if(chunk == NULL || chunk_bins == NULL)
            local_error_flag = 1;

        MPI_Reduce(&local_error_flag, &global_error_flag, 1, MPI_INT, MPI_MAX, 0, MPI_COMM_WORLD);
        if(mpi_rank == 0)
        {
            if(global_error_flag)
                strcpy(error_message, "Cannot allocate memory for chunk in one of the procs.");
        }
    }

    MPI_Bcast(&global_error_flag, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if(!global_error_flag)
    {
        MPI_Bcast(&column, 1, MPI_INT, 0, MPI_COMM_WORLD);
        MPI_Bcast(&num_bins, 1, MPI_INT, 0, MPI_COMM_WORLD);
        MPI_Bcast(&data_max, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
        MPI_Bcast(&data_min, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

        bin_width = (data_max - data_min) / (num_bins - 1);
        current_pos_in_file = 1;
        while(current_pos_in_file < file_size)
        {
            if(mpi_rank == 0)
            {
                if(current_pos_in_file + buffer_size >= file_size)
                {
                    buffer_size = (file_size - current_pos_in_file);
                    if(buffer_size == 0)
                        file_read_done_flag = 2;
                    else
                    {
                        file_read_done_flag = 1;
                        chunk_size = (int) (buffer_size / mpi_num_procs);
                    }
                }
                if(file_read_done_flag < 2)
                {
         //           printf("Reading file at %ld of %ld, buffer size is %ld...\n", current_pos_in_file, file_size, buffer_size);
                    // allocate memory to contain a chunk of file:
                    file_read_result = fread(buffer, 1, buffer_size, data_file);
                    if (file_read_result != buffer_size)
                    {
                        global_error_flag = 1;
                        sprintf(error_message, "Cannot read from file. diff = %ld", buffer_size-file_read_result);
                    }
                    if(first_read_from_file_flag == 1)
                    {
                        //Copy the initial portion of string in buffer to a new buffer to process number of lines.
                        memcpy(num_cols_buffer, buffer, 255);
                        getNumColsInLine(num_cols_buffer, &num_cols_in_line);
                        first_read_from_file_flag = 0;
                    }
                }
                current_pos_in_file += buffer_size;
            }
            MPI_Bcast(&file_read_done_flag, 1, MPI_INT, 0, MPI_COMM_WORLD);
            if(file_read_done_flag > 1)
                break;
            MPI_Bcast(&global_error_flag, 1, MPI_INT, 0, MPI_COMM_WORLD);
            if(!global_error_flag)
            {
                MPI_Bcast(&num_cols_in_line, 1, MPI_INT, 0, MPI_COMM_WORLD);
                MPI_Bcast(&current_pos_in_file, 1, MPI_LONG, 0, MPI_COMM_WORLD);
                MPI_Scatter(buffer, chunk_size, MPI_CHAR, chunk, chunk_size, MPI_CHAR, 0, MPI_COMM_WORLD);

//                getMinMaxInChunk(chunk, column, num_cols_in_line, &chunk_min, &chunk_max, &chunk_processed_line_count, &chunk_discarded_line_count);
                // clear bins
                for(i = 0;i < num_bins;i++)
                {
//                    printf("Rank %d, bin %d of %d.\n", mpi_rank, i, num_bins);
                    chunk_bins[i] = 0;
                }

                sortDataIntoBins(chunk, column, num_cols_in_line, data_min, data_max, bin_width, chunk_bins, &chunk_processed_line_count, &chunk_discarded_line_count);

//                MPI_Reduce(&chunk_min, &buffer_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
//                MPI_Reduce(&chunk_max, &buffer_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
                MPI_Reduce(chunk_bins, buffer_bins, num_bins, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
                MPI_Reduce(&chunk_processed_line_count, &buffer_processed_line_count, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
                MPI_Reduce(&chunk_discarded_line_count, &buffer_discarded_line_count, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
                if(mpi_rank == 0)
                {
//                    if(buffer_min < data_min)
//                        data_min = buffer_min;
//                    if(buffer_max > data_max)
//                        data_max = buffer_max;
                    for(i = 0;i < num_bins;i++)
                        data_bins[i] += buffer_bins[i];
                    data_processed_line_count += buffer_processed_line_count;
                    data_discarded_line_count += buffer_discarded_line_count;
                }
            }
        }
    }

    if(chunk)
        free(chunk);
    if(chunk_bins)
        free(chunk_bins);

    if(mpi_rank == 0)
    {
        if(!global_error_flag)
        {
//            printf("data min = %e, data max = %e\n", data_min, data_max);
            printf("# Output of %s %s %s %s %s %s\n", argv[0], argv[1], argv[2], argv[3], argv[4], argv[5]);
            printf("# Data file size = %s\n", file_size_string);
            printf("# Number of lines processed = %ld\n", data_processed_line_count);
            printf("# Number of lines discarded = %ld\n", data_discarded_line_count);
            for(i = 0;i < num_bins;i++)
                printf("%.5lf\t%.5lf\n", data_min + bin_width*i, ((double)data_bins[i])/((double)data_processed_line_count));
        }
        // terminate
        if(data_file)
            fclose (data_file);
        if(buffer)
            free (buffer);

        if(buffer_bins)
            free(buffer_bins);
        if(data_bins)
            free(data_bins);

        if(global_error_flag)
            printf("\n%s\n", error_message);
    }
    MPI_Finalize();
    return 0;
}
