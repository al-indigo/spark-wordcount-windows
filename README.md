## Spark word count with windows

How to run on standalone cluster:

* cd project root
* mvn package
* spark-submit --class ru.ispras.Main --master \<spark_master> target/scala-wc-windows-0.1-SNAPSHOT.jar \<src_dir> \<res_file> \<window_size>

For example, how I run it on a dev machine:

`mvn package && ~/spark-1.6.1-bin-hadoop2.6/bin/spark-submit --class ru.ispras.Main --master spark://bluenile:7077 target/scala-wc-windows-0.1-SNAPSHOT.jar file:////home/ars/tmp/spark-wordcount-windows/data /home/ars/tmp/res.txt 1`
