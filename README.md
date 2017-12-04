This application aims to detect breaking points in a genome assembly quickly
with parallelization brought by Spark.


Tested for
[Spark-2.2](https://spark.apache.org/releases/spark-release-2-2-0.html) on a
single node with 128 cores and 2TB RAM without Apache Haddop installation.
Written in Scala-2.11. Built with
[sbt-1.0.2](http://www.scala-sbt.org/download.html).


# Usage

1. Build the jar (skip this step if you have the jar file already)

    ```
    git clone git@github.com:zyxue/sparkling-breakpoint.git
    cd sparkling-breakpoint
    sbt package
    ```

    A jar named `sparklingbreakpointapp` will be created under `target/scala-2.11`.

2. Submit a spark job,

    ```
    spark-submit --driver-memory 300G --master local[*]  \
        /path/to/target/scala-2.11/sparklingbreakpointapp_2.11-0.1.0.jar \
        /path/to/extent_file.csv \
        /path/to/output.csv \
        <depthCutoff> \
        <moleculeSize> \
        <maxMoleculeExtent> \
        2>&1 | tee out.log
    ```

    Modify the above arguments accordingly. Lots of log will produced, so better
    save log with `tee` for further debugging purpose.

    Output will be saved in csv/parquet format, the program will choose the format
    according to the extension of the output file specified. 

    If the output is saved in parquet format, for further investigation with
    Python data science stack, you could use
    [pyarrow](https://arrow.apache.org/docs/python/) to read the parquet file.
    Of course, you could also do all the analysis in Spark, esp. when the data
    size is large.


    **Caveat** 

    When running on a single node, I noticed a big variance in terms of runtime.
    For a human 851M extent file, I've seen ~1.5 min to ~30 min. I assume it
    highly depends on the IO condition and how the input is cached when the
    program is running. When it's slow, it seems to be mainly slow for IO. If
    you have insight about the details, please do let me know.


# Development

Handy feature from [sbt](http://www.scala-sbt.org/): auto
compilation/test/package upon change in the source text file.

```
sbt
>~compile
>~test
>~package
````

# A brief intro to how Spark works and its application in breakpoint detection on a single node

[Spark is a fast and general-purpose cluster computing system](https://spark.apache.org/docs/latest/).
It makes programming parallel applications relatively straightforward. In
contrast to the more conventional MPI-based API, which is commonly seen in a
high performance computing (HPC) setting, Spark API are designed at much
higher-level, which allow developers to focus more on the application logic
rather than low-level communications among different workers, exception
handling, etc. Please see
[Jonathan Dursi's post](https://www.dursi.ca/post/hpc-is-dying-and-mpi-is-killing-it.html)
for deeper insight on this topic.

I think the idea for parallel processing data (esp. big data) is relatively
simple. Given that we have many cores, which by the way could be either within a
single node or distributed across multiple nodes, we would like to split the
data into multiple smaller partitions and send them to all cores evenly for
parallel processing, and then aggregate their results into a single one, which
is what we are interested (e.g.
[the classic word counting example](https://dzone.com/articles/word-count-hello-word-program-in-mapreduce)).
This model is named MapReduce and
[published](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pd).
by Goolge in 2004.

Spark reads data in parallel, usually from a distributed file system like
[HDFS](http://hadoop.apache.org/). But when the Spark application is run on a
single node, and reading data from a more conventional file system, the reading
process seems to choke easily, which could be the reason to the big variance in
the aforementioned caveat, esp. when the file size is not too small. As the
[Hadoop wikipedia page](https://en.wikipedia.org/wiki/Apache_Hadoop) puts it:

>This (HDFS) allows the dataset to be processed faster and more efficiently than it
>would be in a more conventional supercomputer architecture that relies on a
>parallel file system where computation and data are distributed via high-speed
>networking

<!-- Need full rewrite after understanding better the driver/executor/RDD stuffs -->

<!-- During a Spark application run, there is one driver, which -->
<!-- [declares the transformations and actions on the RDDs](https://stackoverflow.com/questions/24637312/spark-driver-in-apache-spark), -->
<!-- (RDD is short for resilient distributed dataset, the fundamental data structure -->
<!-- used to represent data in Spark), and there could be multiple executors.) But in -->
<!-- the single-node case, there is one only 1 executor (maybe tunable ?) with 128 -->
<!-- cores, so Spark driver split the data into 128 partitions and sends 128 tasks -->
<!-- (one task per partition) to the executor, which works 128 partitions in -->
<!-- parallel. Note that task is the atomic element in a Spark context, and for a -->
<!-- given partition, only one task can be on it at a time. After the computation is -->
<!-- done on each partition, they are aggregated to obtain the result needed. -->


That's basically (or at least a big part of) what Spark enables. In this
breakpoint detection task in particular. The Spark application reads in an
extent file (e.g. in CSV-like format) which describes the beginning and ending
position of every [10X](https://en.wikipedia.org/wiki/10x_Genomics) molecule on
the corresponding contig it aligns to. The csv file is split into partitions
depending on the system setup. For example, on a single node 128 cores, the file
is split into 128 partitions.

During the map stage, each molecule extent gets converted into its coverage
representation with two point coverage transtions (PCTs). PCT is what I used to
represent coverage efficiently. It is basically a tuple in the format of
(position, coverage, nextCoverage), which tells the coverage of the current
position and that of the next position. In between two neighbouring PCTs, the
coverage is uniform. E.g. suppose a molecule's beginning and ending coordinates
are 10,001 and 20,001, which equivalently means that the coverage of region
[10,0001, 20,001) brought by this molecule alone is 1. Note that the poistion at
20,0001 is actually not included. Such coverage information can be efficiently
represented as two PCTs, (10,000, 0, 1) and (20,000, 1, 0), which means the
coverage transitions from 0 to 1 at position 10,000 and then 1 to 0 at 20,000,
instead of a 10k long array with all ones. In a 0-based coordinate system, the
lowest position in PCT is -1 where the coverage is always zero. Actually, to
represent a coverage, at least two PCTs are needed to represent the transitions
from 0 to x and then x back to 0. This is why each molecule in the extent file
gets converted to two PCTs.

During the reduce stage, all PCTs from all molecules are simply concatenated.

After the reduce, at the finishing stage, the concatenated PCTs are sorted, and
then consolidated to represent the sum of coverages from all molecules. In
contrast to region based way of representing coverage information, still as a
list of PCTs. I find it easier to merge all PCTs than to merge multiple
overlapped regions.

To detect potential breakpoints, we just need to find PCTs that transition
through the specified `depthCutoff`, and maybe some other criteria (e.g. <
`maxMoleculeSpan`).
