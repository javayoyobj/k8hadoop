package com.zj.kafka.k8hadoop;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class KafkaPartition implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4794779956660748284L;
	private String id;
	private List<String> seeds = new ArrayList<String>();
	
	public KafkaPartition() {
		// TODO Auto-generated constructor stub
	}
	
	public KafkaPartition(String id) {
		this.id = id;
	}
	
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getSeeds() {
		return seeds;
	}

	public void setSeeds(List<String> seeds) {
		this.seeds = seeds;
	}

	
}
