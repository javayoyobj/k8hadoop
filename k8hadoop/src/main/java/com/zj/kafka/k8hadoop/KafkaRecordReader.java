package com.zj.kafka.k8hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import kafka.log.Log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRecordReader extends
		RecordReader<LongWritable, BytesWritable> {
	private static Logger LOG = LoggerFactory.getLogger(KafkaRecordReader.class);
    private LongWritable key;
    private BytesWritable value;
	private KafkaContext kafkaContext;
	private TaskAttemptContext context;
	private KafkaSplit kafkaSplit;
	
	private long pos;
	private long count = 0L;

	@Override
	public void close() throws IOException {
		this.kafkaContext.close();
		this.commit();

	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return this.key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {
		return this.value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
      
        return 1.0f;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.context = context;
		this.kafkaSplit = (KafkaSplit) split;

		//Configuration conf = this.context.getConfiguration();

		String[] seedArray = this.kafkaSplit.getSeeds().split(",");
		List<String> seeds = new ArrayList<String>();
		for (int i = 0; i < seedArray.length; i++) {
			seeds.add(seedArray[i]);
		}

		this.kafkaContext = new KafkaContext(this.kafkaSplit.getTopic(),
				Integer.valueOf(this.kafkaSplit.getPartition()), seeds,this.kafkaSplit.getLastCommit());
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		KeyValueInfo nextKeyValue = this.kafkaContext.getNextKeyValue();
        if (nextKeyValue != null) {
        	this.key = nextKeyValue.getKey();
        	this.value = nextKeyValue.getValue();
            pos = nextKeyValue.getCurOffset();
            count++;
            return true;
        }
		return false;
	}

	private void commit() throws IOException {
		if (count == 0L){
			return;
		}
		Configuration conf = this.context.getConfiguration();
		ZkUtils zk = new ZkUtils(conf);
		String group = conf.get("kafka.groupid");
		String partition = this.kafkaSplit.getPartition();
		String topic = this.kafkaSplit.getTopic();
		
		zk.setLastCommit(group, topic, partition, pos, true);
		LOG.info("Commit Temp Path >> Group:{} Topic:{} Partition:{} POS:{}",group,topic,partition,pos);
		zk.close();
	}
	
	public LongWritable getKey() {
		return key;
	}

	public void setKey(LongWritable key) {
		this.key = key;
	}

	public BytesWritable getValue() {
		return value;
	}

	public void setValue(BytesWritable value) {
		this.value = value;
	}

	public KafkaContext getKafkaContext() {
		return kafkaContext;
	}

	public void setKafkaContext(KafkaContext kafkaContext) {
		this.kafkaContext = kafkaContext;
	}

	public TaskAttemptContext getContext() {
		return context;
	}

	public void setContext(TaskAttemptContext context) {
		this.context = context;
	}

	public KafkaSplit getKafkaSplit() {
		return kafkaSplit;
	}

	public void setKafkaSplit(KafkaSplit kafkaSplit) {
		this.kafkaSplit = kafkaSplit;
	}

	public long getPos() {
		return pos;
	}

	public void setPos(long pos) {
		this.pos = pos;
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}
	
	

}
