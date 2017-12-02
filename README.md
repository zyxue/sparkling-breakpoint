This application aims to detect breaking points in a genome assembly quickly
with parallelization brought by Spark.


Tested on
[Spark-2.2](https://spark.apache.org/releases/spark-release-2-2-0.html) compiled
with Scala-2.11, built with [sbt-1.0.2](http://www.scala-sbt.org/download.html).


```
# after cloning, go to the directory
sbt package
```

A jar would be created under `target/scala-2.11`.

Submit a spark job,

```
spark-submit --driver-memory 300G --master local[*]  \
    /path/to/target/scala-2.11/sparklingbreakpoint_2.11-0.1.0.jar \
    /path/to/extent_file.csv \
    /path/to/output.csv \
    <depthCutoff> \
    <moleculeSize> \
    <maxMoleculeExtent> \
    2>&1 | tee out.log
````

Modify the above arguments accordingly. Lots of log will produced, so better
save log for further debugging purpose.

Output is save in csv/parquet format, the program will choose the format
according to the extension of the output file specified. 

For further investigation with Python data science stack, you could use
[pyarrow](https://arrow.apache.org/docs/python/) to read the parquet file.

**Caveat** 

When running on a standalone mode, I noticed a huge variance in terms of
runtime. For a human 851M extent file, I've seen ~1.5 min to ~30min. I assume it
highly depends on the IO condition when the program is running. When it's slow,
it seems to be mainly slow for IO.


# Development

Auto compilation/test/package upon saving text file.

```
sbt
>~compile
>~test
>~package
````

# A brief intro to how Spark works

[Spark is a fast and general-purpose cluster computing system](https://spark.apache.org/docs/latest/).
It enables programming applications to be run in parallel relatively easy. In
contrast to more convention MPI-based API, which is commonly seen in a high
performance computing (HPC) setting, Spark exposes much higher-level APIs, which
allow developers to focus on the application rather than low-level communication
among different processes, and exception handling, etc. Please see
[Jonathan Dursi's post](https://www.dursi.ca/post/hpc-is-dying-and-mpi-is-killing-it.html)
for deep insights on this topic.

I think the idea for parallel processing data (esp. big data) is simple. Given
that we have many cores, which by the way could be either within a single node
or distributed across multiple nodes, we would like split the data into multiple
smaller partitions and send them to each core for processing in parallel, and
then aggregate their results into a single one that we intended. This model is
named MapReduce and
[published](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pd).
by Goolge in 2004.

That's basically (or at least a big part of) what Spark enables. In this
breakpoint detection task in particular. The Spark application reads in a extent
file (e.g. in CSV-like format) which describes the beginning and ending position
on the contig coordinates of every molecule. The reading process is likely (? to
confirm) in parallel, and then the csv file get split into partitions. E.g. when
there are 128 cores on a machine in standalone mode, the file is split into 128
partitions. 

During a Spark application run, there is one driver, and there could be multiple
executors. In the standalone mode, there is one 1 executor with 128 cores, so
Spark driver sends 128 tasks to the executor, which works 128 partitions
concurrently. Note that task is the atomic element (ref) in a Spark context, and
for one partition, only one task can be on it.

For breakpoint detection in particular, during the map stage, each molecule
extent gets converted into two point coverage transtions (PCTs), which basically
tells where the coverage transitioned from 0 to 1 and then 1 back to 0. During
the reduce stage, all PCTs from all molecules are simply concatenated. Afte the
reduce, we post procress the concatenated PCTs to become coverage, still
represented by a list of PCTs per contig. Last, it looks for specific PCTs that
passes the specified `depthCutoff` to be reported as breakpoints.
