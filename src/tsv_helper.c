/*
 * =====================================================================================
 *
 *       Filename:  helper.c
 *
 *    Description:  Helper functions for processing TSV files
 *
 *        Version:  1.0
 *        Created:  01/14/2019 02:41:09 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Sachin Krishnan T V (sachin@physics.iitm.ac.in)
 *   Organization:
 *
 * =====================================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>


void getSizeInHumanReadableForm(long size, char* sizeString)
{
    double sizeValue = size;
    char unitString[][3] = {"B", "kB", "MB", "GB", "TB"};
    int unit = 0; // 0 - B, 1 - kB, 2 - MB, 3 - GB, 4 - TB. Lets hope we wont go beyond that.
    while(sizeValue >=1024.0)
    {
        sizeValue /= 1024.0;
        unit++;
    }
    sprintf(sizeString, "%lf %s", sizeValue, unitString[unit]);
    return;
}

void getMinMaxInChunk(char *buffer, int col, int ncols, double *min, double *max, long *processed_line_count, long *discarded_line_count)
{
    int col_count;
    char *temp;
    char *token;
    char *buffer_step = buffer;
    double val;
    *min = 1e42;
    *max = -1e42;
    *processed_line_count = 0;
    *discarded_line_count = 0;
    // the chunk is now loaded in the memory buffer.
    while((temp = strtok_r(buffer_step, "\n", &buffer_step)))
    {
        col_count = 0;
        while((token = strtok_r(temp, " \t", &temp)))
        {
            if(col_count == col)
                val = atof(token);
            col_count++;
        }
        if(col_count == ncols)
        {
            (*processed_line_count)++;
            if(val > *max)
                *max = val;
            else if(val < *min)
                *min = val;
        }
        else
        {
            (*discarded_line_count)++;
        }
    }
    return;
}

void sortDataIntoBins(char *buffer, int col, int ncols, double min, double max, double binwidth, long *value_bins, long *processed_line_count, long *discarded_line_count)
{
    int col_count;
    char *temp;
    char *token;
    double val;
    *processed_line_count = 0;
    *discarded_line_count = 0;
    // the chunk is now loaded in the memory buffer.
    while((temp = strtok_r(buffer, "\n", &buffer)))
    {
        col_count = 0;
        while((token = strtok_r(temp, " \t", &temp)))
        {
            if(col_count == col)
                val = atof(token);
            col_count++;
        }
        if(col_count == ncols)
        {
            (*processed_line_count)++;
            if(val >= min && val <= max)
            {
                value_bins[(int)((val - min) / binwidth)]++;
            }
        }
        else
        {
            (*discarded_line_count)++;
        }
    }
    return;
}


void getNumColsInLine(char *buffer, int *ncols)
{
    char *temp;
    char *token;
    temp = strtok_r(buffer, "\n", &buffer);
    *ncols = 0;
    while((token = strtok_r(temp, " \t", &temp)))
    {
        (*ncols)++;
    }
}


