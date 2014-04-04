package com.zj.kafka.k8hadoop;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkUtils implements Closeable {
	
    private static Logger LOG = LoggerFactory.getLogger(ZkUtils.class);
    
    private static final String CONSUMERS_PATH = "/consumers";
    private static final String BROKER_IDS_PATH = "/brokers/ids";
    private static final String BROKER_TOPICS_PATH = "/brokers/topics";
	
	private ZkClient zkClient;
	private Map<String, String> brokers;
    public ZkUtils(Configuration config) {
    	String zk = config.get("kafka.zk.connect");
    	zkClient = new ZkClient(zk, 10000, 10000, new StringSerializer() );
    }
	
	@Override
	public void close() throws IOException {
        if (zkClient != null) {
        	zkClient.close();
        }
	}
	
    private List<String> getChildrenParentMayNotExist(String path) {
        try {
            List<String> children = zkClient.getChildren(path);
            return children;
        } catch (ZkNoNodeException e) {
            return new ArrayList<String>();
        }
    }
    
    private String getOffsetsPath(String group, String topic, String partition) {
        return CONSUMERS_PATH + "/" + group + "/offsets/" + topic + "/" + partition;
    }
    
    private String getTempOffsetsPath(String group, String topic, String partition) {
        return CONSUMERS_PATH + "/" + group + "/offsets-temp/" + topic + "/" + partition;
    }
    
    private String getTempOffsetsPath(String group, String topic) {
        return CONSUMERS_PATH + "/" + group + "/offsets-temp/" + topic ;
    }
	
    public String getBroker(String id) {
        if (brokers == null) {
            brokers = new HashMap<String, String>();
            List<String> brokerIds = getChildrenParentMayNotExist(BROKER_IDS_PATH);
            for(String bid: brokerIds) {
                String data = zkClient.readData(BROKER_IDS_PATH + "/" + bid);
                try{
	    			ObjectMapper m = new ObjectMapper();  
	    			JsonNode rootNode = m.readValue(data, JsonNode.class);
	    			JsonNode hostNode = rootNode.path("host");
	    			JsonNode portNode = rootNode.path("port");
	                brokers.put(bid, hostNode.getTextValue()+":"+portNode.getIntValue());
                }catch(Exception e){
                	LOG.error(e.getMessage());
                }

            }
        }
        return brokers.get(id);
    }
	
	public List<KafkaPartition> getPartitions(String topic) {
		List<KafkaPartition> partitions = new ArrayList<KafkaPartition>();
		try{
			String topicData = zkClient.readData(BROKER_TOPICS_PATH + "/" + topic);
			
			ObjectMapper m = new ObjectMapper();  
			JsonNode rootNode = m.readValue(topicData, JsonNode.class);
			JsonNode partitionsNode = rootNode.path("partitions");
			for (Iterator<Entry<String, JsonNode>> iterator = partitionsNode.getFields(); iterator.hasNext();) {
				Entry<String, JsonNode> partitionNode = iterator.next();
				KafkaPartition partition = new KafkaPartition(partitionNode.getKey());
				for (Iterator<JsonNode> iterator2 = partitionNode.getValue().iterator(); iterator2
						.hasNext();) {
					JsonNode brokerNode =iterator2.next();
					String broker = this.getBroker(brokerNode.getValueAsText());
					if(broker != null){
						partition.getSeeds().add(broker);
					}
				}
				partitions.add(partition);
			}
		}catch(Exception e){
			
		}
		
		return partitions;
	}
	
	public KafkaTopic getKafkaTopic(String topic){
		KafkaTopic kafkaTopic = new KafkaTopic();
		kafkaTopic.setTopic(topic);
		kafkaTopic.setPartitions(this.getPartitions(topic));
		return kafkaTopic;
	}
	
	
    public boolean commit(String group, String topic) {
        List<String> partitions = getChildrenParentMayNotExist(getTempOffsetsPath(group, topic));
        for(String partition: partitions) {
            String path = getTempOffsetsPath(group, topic, partition);
            String offset = this.zkClient.readData(path);
            setLastCommit(group, topic, partition, Long.valueOf(offset), false);
            this.zkClient.delete(path);
        }
        return true;
    }
    
    public long getLastCommit(String group, String topic, String partition) {
        String znode = getOffsetsPath(group ,topic ,partition);
        String offset = this.zkClient.readData(znode, true);
        
        if (offset == null) {
        	//ZhangJun Update
            return 0L;
        }
        return Long.valueOf(offset)+1;
    }
    
    public void setLastCommit(String group, String topic, String partition, long commit, boolean temp) {
        String path = temp? getTempOffsetsPath(group ,topic ,partition)
                        : getOffsetsPath(group ,topic ,partition);
        if (!zkClient.exists(path)) {
        	zkClient.createPersistent(path, true);
        }
        zkClient.writeData(path, commit);
    }

    static class StringSerializer implements ZkSerializer {

        public StringSerializer() {}
        @Override
        public Object deserialize(byte[] data) throws ZkMarshallingError {
            if (data == null) return null;
            return new String(data);
        }

        @Override
        public byte[] serialize(Object data) throws ZkMarshallingError {
            return data.toString().getBytes();
        }
        
    }
}
