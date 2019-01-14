# TSVHistogram-mpi
Generates histogram from data in Tab Separated Value (TSV) format. Data processing is parallelized using MPI.

## Use case
When you need to make histograms out of data in one column in a large data file in TSV format. 

## Dependencies
OpenMPI

## How to use it?
* Navigate to the directory
* Compile using `make` 
* Run `mpirun -np 1 ./tsv_hist.out <column> <min-value> <max-value> <num-bins> <TSVfile>`

- <column> specifies the column in the TSV file which contains data for histogram
- <min-value> and <max-value> specifies the range of the data
- <num-bins> is the number of bins required
- <TSVfile> is the path to the data file in TSV format

## How much time does it take?
When tested with data files of size (300MB to 32GB), the time taken to generate histogram was:
![Time Benchmark](https://raw.githubusercontent.com/sachu92/TSVHistogram-mpi/master/benchmark/time-benchmark.png)

## What kind of speed up can we get with multiple processors?

Speed up with respect to time taken in single processor:
![Speed Benchmark](https://raw.githubusercontent.com/sachu92/TSVHistogram-mpi/master/benchmark/speed-benchmark.png)

