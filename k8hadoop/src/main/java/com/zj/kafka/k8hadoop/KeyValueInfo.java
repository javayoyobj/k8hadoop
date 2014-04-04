package com.zj.kafka.k8hadoop;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

public class KeyValueInfo {
	private LongWritable key;
	private BytesWritable value;
	private long curOffset;
	
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
	public long getCurOffset() {
		return curOffset;
	}
	public void setCurOffset(long curOffset) {
		this.curOffset = curOffset;
	}
	
	
}
