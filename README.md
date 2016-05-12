## Spark word count with windows

How to run on standalone cluster:

* cd project root
* mvn package
* spark-submit --class ru.ispras.Main --master \<spark_master> target/scala-wc-windows-0.1-SNAPSHOT.jar \<fs_type> \<src_dir> \<res_dir> \<window_size>

fs_type can be "local" now and also "hdfs", "swift" in future.

For example, how I run it on a dev machine:

`mvn package && ~/spark-1.6.1-bin-hadoop2.6/bin/spark-submit --class ru.ispras.Main --master spark://Arsmint:7077 target/scala-wc-windows-0.1-SNAPSHOT.jar local /home/ars/tmp/spark-wc-windows/data /home/ars/tmp/spark-wc-windows/res 2`
