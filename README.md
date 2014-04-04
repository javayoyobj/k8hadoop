k8hadoop
========

k8hadoop is to load data into HDFS in Kafka mapreduce jobs

Overview
========

1.Topic offsets stored in Zookeeper.

Run
========

First kafka_lib added to the hadoop classpath

java -cp k8hadoop.jar:\`hadoop classpath\` com.zj.kafka.k8hadoop.HadoopConsumer -z <zookeeper> -t <topic> target_hdfs_path
