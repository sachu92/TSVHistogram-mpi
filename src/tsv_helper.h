/*
 * =====================================================================================
 *
 *       Filename:  tsv_helper.h
 *
 *    Description:  TSV helper functions
 *
 *        Version:  1.0
 *        Created:  01/14/2019 02:44:17 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  Sachin Krishnan T V (sachin@physics.iitm.ac.in)
 *   Organization:
 *
 * =====================================================================================
 */

#ifndef __TSV_HELPER__
#define __TSV_HELPER__

void getSizeInHumanReadableForm(long size, char* sizeString);
void getMinMaxInChunk(char *buffer, int col, int ncols, double *min, double *max, long *processed_line_count, long *discarded_line_count);
void sortDataIntoBins(char *buffer, int col, int ncols, double min, double max, double binwidth, double *value_bins, long *processed_line_count, long *discarded_line_count);
void getNumColsInLine(char *buffer, int *ncols);

#endif
