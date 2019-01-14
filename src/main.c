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
    int progress_width;
    size_t file_read_result;
    FILE * data_file = NULL;
    long file_size;
    long buffer_size;
    long chunk_size;
    long current_pos_in_file;
    long chunk_processed_line_count;
    long buffer_processed_line_count = 0;
    long data_processed_line_count = 0;
    long chunk_discarded_line_count;
    long buffer_discarded_line_count = 0;
    long data_discarded_line_count = 0;

    char *buffer = NULL;
    char *chunk = NULL;
    char error_message[100];
    char file_size_string[100];
    char num_cols_buffer[256];
    double chunk_min, chunk_max;
    double buffer_min, buffer_max;
    double data_min = 1e42, data_max = -1e42;

    // Initialize MPI environment
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_num_procs);

    chunk_size = 50*BYTESINMB;
    buffer_size = mpi_num_procs*chunk_size;
    if(mpi_rank == 0) // root node
    {
        if(argc < 4)
        {
            global_error_flag = 1;
            sprintf(error_message, "Usage: %s <column> <num-bins> <file>\n", argv[0]);
        }
        if(!global_error_flag)
        {
            column = atof(argv[1]);
            num_bins = atof(argv[2]);
            data_file = fopen(argv[3], "r");
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
            printf("Total file size : %s\n", file_size_string);

            buffer = (char*) malloc(sizeof(char)*buffer_size);
            if(buffer == NULL)
            {
                global_error_flag = 1;
                strcpy(error_message, "Cannot allocate memory for buffer.");
            }
        }
    }

    MPI_Bcast(&global_error_flag, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if(!global_error_flag)
    {
        MPI_Bcast(&file_size, 1, MPI_LONG, 0, MPI_COMM_WORLD);
        chunk = (char *) malloc(sizeof(char)*chunk_size);
        if(chunk == NULL)
        {
            local_error_flag = 1;
        }
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
                  /*   progress_width = (int)(current_pos_in_file*50/file_size) + 1;
                    printf("\rProcessing [");
                    for(i = 1;i <= 50;i++)
                        if(i<progress_width)
                            printf("#");
                        else
                            printf(" ");
                    printf("]");
                    fflush(stdout);
*/
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

                getMinMaxInChunk(chunk, column, num_cols_in_line, &chunk_min, &chunk_max, &chunk_processed_line_count, &chunk_discarded_line_count);

                MPI_Reduce(&chunk_min, &buffer_min, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
                MPI_Reduce(&chunk_max, &buffer_max, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
                MPI_Reduce(&chunk_processed_line_count, &buffer_processed_line_count, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
                MPI_Reduce(&chunk_discarded_line_count, &buffer_discarded_line_count, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
                if(mpi_rank == 0)
                {
                    if(buffer_min < data_min)
                        data_min = buffer_min;
                    if(buffer_max > data_max)
                        data_max = buffer_max;
                    data_processed_line_count += buffer_processed_line_count;
                    data_discarded_line_count += buffer_discarded_line_count;
                }
            }
        }
    }

    if(chunk)
        free(chunk);

    if(mpi_rank == 0)
    {
        if(!global_error_flag)
        {
 /*           printf("\rProcessing [");
            for(i = 1;i <= 50;i++)
                printf("#");
            printf("]\n");
*/
            printf("total number of lines processed = %ld\n", data_processed_line_count);
            printf("total number of lines discarded = %ld\n", data_discarded_line_count);
            printf("data min = %e, data max = %e\n", data_min, data_max);
        }
        // terminate
        if(data_file)
            fclose (data_file);
        if(buffer)
            free (buffer);
        if(global_error_flag)
            printf("\n%s\n", error_message);
    }
    MPI_Finalize();
    return 0;
}
