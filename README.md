k8hadoop
========

It is load data into HDFS from kafka mapreduce jobs.

Overview
========

1.Topic offsets stored in Zookeeper.

2.Run into YARN

3.Support 2.8.0 version kafka

Run
========

First kafka_lib added to the hadoop classpath

java -cp k8hadoop.jar:\`hadoop classpath\` com.zj.kafka.k8hadoop.HadoopConsumer -z \<zookeeper\> -t \<topic\> target_hdfs_path
