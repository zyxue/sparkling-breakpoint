This application aims to detect breaking points in a genome assembly quickly
with massive parallelization brought by Spark.


tested on
[Spark-2.2](https://spark.apache.org/releases/spark-release-2-2-0.html) compiled
with Scala-2.11, built with [sbt-1.0.2](http://www.scala-sbt.org/download.html).


```
# after cloning, go to the directory
sbt package
```

A jar would be created under `target/scala-2.11`.

Submit the spark job,

```
spark-submit --driver-memory 300G --master local[2]  \
    /path/to/target/scala-2.11/sparklingbreakpoint_2.11-0.1.0.jar \
    /path/to/span_file.csv \
    /path/to/output.parquet \
    2>&1 | tee out.log
````

Modify the above arguments accordingly. Lots of log are produced, so better save
log for further debugging purpose.

When running on a standalone node, I specified a big chunk of memory.

Output is save in parquet format. For further investigation with Python data
science stack, you could use [pyarrow](https://arrow.apache.org/docs/python/) to
read the parquet file.

# Development

Auto compilation

```
sbt
>~package
````
