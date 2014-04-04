package com.zj.kafka.k8hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSplit extends InputSplit implements Writable{
	private static Logger LOG = LoggerFactory.getLogger(KafkaSplit.class);
	private String topic;
	private String partition;
	private String seeds="";
	private long lastCommit;
	
	public KafkaSplit() {}
	
    public KafkaSplit(String topic,KafkaPartition kafkaPartition, long lastCommit) {
    	this.topic = topic;
    	this.partition = kafkaPartition.getId();
    	this.lastCommit = lastCommit;
    	
    	for (String seed : kafkaPartition.getSeeds()) {
			this.seeds +=","+seed;
			
		}
    	
    	LOG.info("Topic:{} Partition:{} LastCommit:{}  Seeds:{} ",this.topic,this.partition,this.lastCommit,this.seeds.substring(1));
    	this.seeds = this.seeds.substring(1);
    }
	
	@Override
	public void readFields(DataInput in) throws IOException {
        this.topic = Text.readString(in);
        this.partition = Text.readString(in);
        this.seeds = Text.readString(in);
        this.lastCommit = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
        Text.writeString(out, topic);
        Text.writeString(out, partition);
        Text.writeString(out, seeds);
        out.writeLong(lastCommit);
		
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return Long.MAX_VALUE;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[] {"hadoop2.localdomain"};
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getSeeds() {
		return seeds;
	}

	public void setSeeds(String seeds) {
		this.seeds = seeds;
	}

	public long getLastCommit() {
		return lastCommit;
	}

	public void setLastCommit(long lastCommit) {
		this.lastCommit = lastCommit;
	}

	public String getPartition() {
		return partition;
	}

	public void setPartition(String partition) {
		this.partition = partition;
	}
	
	

	
}
