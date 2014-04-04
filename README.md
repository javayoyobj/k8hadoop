k8hadoop hadoop consumer
========

k8hadoop is to load data into HDFS in Kafka mapreduce jobs.

Overview
========

1.Topic offsets stored in Zookeeper.

2.Run into YARN

Run
========

First kafka_lib added to the hadoop classpath

java -cp k8hadoop.jar:\`hadoop classpath\` com.zj.kafka.k8hadoop.HadoopConsumer -z <zookeeper> -t <topic> target_hdfs_path
