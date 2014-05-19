package com.rocketmq.community.storm.support;

import java.io.Serializable;

/**
 * 消费的元数据
 * @author 47626399@qq.com
 *
 */
public class RocketBatchSpoutMeta implements Serializable{
	
	//此次消费的偏移量
	long offset;
	//下次消费的偏移量
	long nextOffset;

	public RocketBatchSpoutMeta(long offset, long nextOffset) {
		this.offset = offset;
		this.nextOffset = nextOffset;
	}

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public long getNextOffset() {
		return nextOffset;
	}

	public void setNextOffset(long nextOffset) {
		this.nextOffset = nextOffset;
	}
	
	public int getBatchSize(){
		return (int)(nextOffset-offset+1);
	}
	
}
