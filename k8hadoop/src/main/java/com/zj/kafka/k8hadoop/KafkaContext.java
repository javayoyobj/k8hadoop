package com.zj.kafka.k8hadoop;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaContext implements Closeable{
	private static Logger LOG = LoggerFactory.getLogger(KafkaContext.class);
	private String topic;
	private int partition;
	private long curOffset;
	private long lastCommit;
	private long startOffset;
	private List<String> seeds;
	private List<String> replicaBrokers = new ArrayList<String>();
	private Iterator<MessageAndOffset> iterator;
	
	public KafkaContext() {
	}
	
	public KafkaContext(String topic,int partition,List<String> seeds,long lastCommit) {
		this.topic = topic;
		this.partition = partition;
		this.seeds = seeds;
		this.lastCommit = lastCommit;
		this.startOffset = lastCommit;
		this.curOffset = lastCommit;
	}
	
	public Iterator<MessageAndOffset> createIterable(long offset){
			SimpleConsumer consumer = null;
		try{
			PartitionMetadata metadata = findLeader(this.seeds);
	
			if (metadata == null) {
				LOG.info("Can't find metadata for Topic and Partition. Exiting");
				return null;
			}
	
			if (metadata.leader() == null) {
				LOG.info("Can't find Leader for Topic and Partition. Exiting");
				return null;
			}
			
			String leadBroker = metadata.leader().host();
			int port = Integer.valueOf(this.seeds.get(0).split(":")[1]);
			String clientName = "Client_" + this.topic + "_" + this.partition;
			
			consumer = new SimpleConsumer(leadBroker, port,
					100000, 64 * 1024, clientName);
			
			long lastOffset = this.getLastOffset(consumer, leadBroker, port, kafka.api.OffsetRequest.EarliestTime(), clientName);
			if(offset < lastOffset){
				LOG.info("Last offset is "+lastOffset+" ,offset is "+offset);
				return null;
			}
			
			FetchRequest req = new FetchRequestBuilder().clientId(clientName)
					.addFetch(this.topic, this.partition, offset,1024*1024).build();
			
			FetchResponse fetchResponse = consumer.fetch(req);
			
			if (fetchResponse.hasError()) {
				short code = fetchResponse.errorCode(this.topic, this.partition);
				if (code == ErrorMapping.OffsetOutOfRangeCode())  {
					LOG.info("Offset out of range. code:{} offset:{}",code,this.lastCommit);
				}else{
					LOG.info("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
				}
				consumer.close();
				return null;
			}
			
			return fetchResponse.messageSet(this.topic, this.partition).iterator();
		
		}finally{
			if(consumer != null){
				consumer.close();
			}
		}
	}
	
	public Iterator<MessageAndOffset> createIterable(){
		SimpleConsumer consumer = null;
		try{
			PartitionMetadata metadata = findLeader(this.seeds);
	
			if (metadata == null) {
				LOG.info("Can't find metadata for Topic and Partition. Exiting");
				return null;
			}
	
			if (metadata.leader() == null) {
				LOG.info("Can't find Leader for Topic and Partition. Exiting");
				return null;
			}
			
			String leadBroker = metadata.leader().host();
			int port = Integer.valueOf(this.seeds.get(0).split(":")[1]);
			String clientName = "Client_" + this.topic + "_" + this.partition;
			
			consumer = new SimpleConsumer(leadBroker, port,
					100000, 64 * 1024, clientName);
			
			long lastOffset = this.getLastOffset(consumer, leadBroker, port, kafka.api.OffsetRequest.EarliestTime(), clientName);
			if(this.lastCommit < lastOffset){
				LOG.info("Last offset is "+lastOffset+" ,Last commit is "+this.lastCommit);
				return null;
			}
			
			FetchRequest req = new FetchRequestBuilder().clientId(clientName)
					.addFetch(this.topic, this.partition, this.lastCommit,1024*1024).build();
			
			FetchResponse fetchResponse = consumer.fetch(req);
			
			if (fetchResponse.hasError()) {
				short code = fetchResponse.errorCode(this.topic, this.partition);
				if (code == ErrorMapping.OffsetOutOfRangeCode())  {
					LOG.info("Offset out of range. code:{} offset:{}",code,this.lastCommit);
				}else{
					LOG.info("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
				}
				consumer.close();
				return null;
			}
			return fetchResponse.messageSet(this.topic, this.partition).iterator();
		}finally{
			if(consumer != null){
				consumer.close();
			}
		}
		
		
	}
	
	private long getLastOffset(SimpleConsumer consumer, String topic,
			int partition, long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,
				partition);

		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
				whichTime, 1));

		OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
				kafka.api.OffsetRequest.CurrentVersion(), clientName);

		OffsetResponse response = consumer.getOffsetsBefore(request);
		if (response.hasError()) {
			System.out
					.println("Error fetching data Offset Data the Broker. Reason: "
							+ response.errorCode(topic, partition));
			return 0;
		}

		long[] offsets = response.offsets(topic, partition);

		return offsets[0];
	}
	
	private PartitionMetadata findLeader(List<String> seeds) {
		PartitionMetadata returnMetaData = null;
		for (String seed : this.seeds) {
			SimpleConsumer consumer = null;
			try{
				String[] seedArray = seed.split(":");
				String host = seedArray[0];
				int port = Integer.valueOf(seedArray[1]);
				
				LOG.info("Host:{} Port:{} Topic:{} Partition:{}",host,port,this.topic,this.partition);
				consumer = new SimpleConsumer(host, port, 10000, 64 * 1024,
						"leaderLookup");
				List<String> topics = new ArrayList<String>();
				topics.add(this.topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				
				TopicMetadataResponse resp = consumer.send(req);
				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == this.partition) {
							returnMetaData = part;
							break;
						}
					}
				}
				
			} catch (Exception e) {
				System.out.println("Error communicating with Broker [" + seed
						+ "] to find Leader for [" + topic
	
						+ ", " + partition + "] Reason: " + e);
				
			} finally {
	
				if (consumer != null)
					consumer.close();
	
			}
		}
		
		if (returnMetaData != null) {
			this.replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				this.replicaBrokers.add(replica.host()+":"+replica.port());
			}
		}
		return returnMetaData;
	}
	
	private boolean hasMore(){
		if(this.iterator == null){
			this.iterator = this.createIterable();
		}
		
		if(this.iterator != null){
			if(!this.iterator.hasNext()){
				this.iterator = this.createIterable(this.curOffset+1);
			}
		}
		
		if(this.iterator == null){
			return false;
		}
		
		return true;
		
	}
	
	public KeyValueInfo getNextKeyValue(){
		if(!this.hasMore()){
			return null;
		}

		if(this.iterator.hasNext()){
			MessageAndOffset messageOffset = iterator.next();
		
	        Message message = messageOffset.message();
	        
			KeyValueInfo keyValueInfo = new KeyValueInfo();
			LongWritable key = new LongWritable();
			BytesWritable value = new BytesWritable();
			
			this.curOffset = messageOffset.offset();
			key.set(this.curOffset);
	        ByteBuffer buffer = message.payload();
	        value.set(buffer.array(), buffer.arrayOffset(), message.payloadSize());
			
			keyValueInfo.setKey(key);
			keyValueInfo.setValue(value);
			keyValueInfo.setCurOffset(this.curOffset);
			
			return keyValueInfo;
		}
		return null;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public List<String> getSeeds() {
		return seeds;
	}

	public void setSeeds(List<String> seeds) {
		this.seeds = seeds;
	}

	public long getLastCommit() {
		return lastCommit;
	}

	public void setLastCommit(long lastCommit) {
		this.lastCommit = lastCommit;
	}

	public long getStartOffset() {
		return startOffset;
	}

	public void setStartOffset(long startOffset) {
		this.startOffset = startOffset;
	}

	public List<String> getReplicaBrokers() {
		return replicaBrokers;
	}

	public void setReplicaBrokers(List<String> replicaBrokers) {
		this.replicaBrokers = replicaBrokers;
	}


	public Iterator<MessageAndOffset> getIterator() {
		return iterator;
	}

	public void setIterator(Iterator<MessageAndOffset> iterator) {
		this.iterator = iterator;
	}

	public long getCurOffset() {
		return curOffset;
	}

	public void setCurOffset(long curOffset) {
		this.curOffset = curOffset;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}
	
	
}
