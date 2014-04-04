package com.zj.kafka.k8hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class KafkaInputFormat extends InputFormat<LongWritable, BytesWritable> {
	
	@Override
	public RecordReader<LongWritable, BytesWritable> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		return new KafkaRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		Configuration conf = context.getConfiguration();
        String topic = conf.get("kafka.topic");
        String group = conf.get("kafka.groupid");
		ZkUtils zk = null;
		try{
			zk = new ZkUtils(conf);
	        KafkaTopic kafkaTopic =  zk.getKafkaTopic(topic);
	        for (KafkaPartition kafkaPartition : kafkaTopic.getPartitions()) {
	        	long lastCommit = zk.getLastCommit(group, topic, kafkaPartition.getId());
	        	InputSplit split = new KafkaSplit(topic,kafkaPartition,lastCommit);
	        	splits.add(split);
			}
		}finally{
			if(zk!=null)
				zk.close();
		}
		return splits;
	}

}
