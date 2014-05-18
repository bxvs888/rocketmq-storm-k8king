package com.rocketmq.community.storm.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.tuple.Fields;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.rocketmq.community.storm.support.RocketBatchSpoutMeta;
import com.rocketmq.community.storm.support.RocketMQConsumerManager;

/**
 * RocketMQ批量获取
 * @author xiangnan.wang@ipinyou.com
 *
 */
public class RocketBatchSpout implements IPartitionedTransactionalSpout<RocketBatchSpoutMeta>{
	
	public static String TX_FIELD = RocketBatchSpout.class.getName() + "/id";
	
	
//	private DefaultMQPullConsumer consumer;
	
	private String topic;
	
	private Fields outFields;
	
	/**
	 * 单批次最大的数据量
	 */
	private int maxBatchSize;
	

	/**
	 * 
	 * @param consumerGroup 消费组名称
	 * @param topic	
	 * @param hosts nameserver address
	 * @throws MQClientException
	 */
	public RocketBatchSpout(String consumerGroup,final String topic,String hosts,int maxBatchSize,Fields outFields) throws MQClientException{
		this.topic=topic;
		this.maxBatchSize=maxBatchSize;
		this.outFields=outFields;
		RocketMQConsumerManager.registConsumer(topic, hosts, consumerGroup);
	}
	
	
	class Coordinator implements IPartitionedTransactionalSpout.Coordinator {
		
		@Override
		public int numPartitions() {
			List mqL=RocketMQConsumerManager.getMessageQueue(topic);
			if(mqL!=null){
				return mqL.size();
			}
			return 0;
		}

		@Override
		public boolean isReady() {
//			try {
//				consumer.fetchSubscribeMessageQueues(RocketBatchSpout.this.topic);
//			} catch (MQClientException e) {
//				e.printStackTrace();
//				return false;
//			}
			return true;
		}

		@Override
		public void close() {
		}
		
	}
	
    class Emitter implements IPartitionedTransactionalSpout.Emitter<RocketBatchSpoutMeta> {
    	
		@Override
		public RocketBatchSpoutMeta emitPartitionBatchNew(
				TransactionAttempt tx, BatchOutputCollector collector,
				int partition, RocketBatchSpoutMeta lastPartitionMeta) {
			long index=0;
			MessageQueue mq=getMessageQueue(partition);
			RocketBatchSpoutMeta meta=null;
			try {
				DefaultMQPullConsumer consumer=RocketMQConsumerManager.getConsumer(topic);
				if(lastPartitionMeta==null) {
					//没有上一次,说明第一次发送
					//获取目前的消费进度
					index = consumer.fetchConsumeOffset(mq, false);
					index=index==-1?0:index;
            	} else {
            		index = lastPartitionMeta.getNextOffset();
            	}
				//获取队列最大的offset
//				long maxOffset=consumer.maxOffset(mq);
				PullResult result=consumer.pullBlockIfNotFound(mq, "*", index, maxBatchSize);
				List<MessageExt> msgL=result.getMsgFoundList();
				if(msgL!=null){
					meta=new RocketBatchSpoutMeta(index,index+msgL.size());
					consumer.updateConsumeOffset(mq,index+msgL.size());
					System.out.println("emitPartitionBatchNew :"+tx.getTransactionId()+"|"+tx.getAttemptId()+"|"+msgL.size());
					for(MessageExt mess:msgL){
						List<Object> toEmit=new ArrayList<Object>();
						toEmit.add(0,tx);
						toEmit.add(mess);
						collector.emit(toEmit);
					}
				}
			}catch (Exception e) {
				e.printStackTrace();
			}
			return meta;
		}

		@Override
		public void emitPartitionBatch(TransactionAttempt tx,
				BatchOutputCollector collector, int partition,
				RocketBatchSpoutMeta partitionMeta) {
			MessageQueue mq=getMessageQueue(partition);
			try {
				DefaultMQPullConsumer consumer=RocketMQConsumerManager.getConsumer(topic);
				PullResult result=consumer.pullBlockIfNotFound(mq, "*", partitionMeta.getOffset(),partitionMeta.getBatchSize());
				List<MessageExt> msgL=result.getMsgFoundList();
				if(msgL!=null){
					consumer.updateConsumeOffset(mq,partitionMeta.getNextOffset());
					System.out.println("emitPartitionBatchNew :"+tx.getTransactionId()+"|"+tx.getAttemptId()+"|"+msgL.size());
					for(MessageExt mess:msgL){
						List<Object> toEmit=new ArrayList<Object>();
						toEmit.add(0,tx);
						toEmit.add(mess);
						collector.emit(toEmit);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			
		}
    	
    }
    
    
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> toDeclare = new ArrayList<String>(outFields.toList());
        toDeclare.add(0, TX_FIELD);
        declarer.declare(new Fields(toDeclare));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.registerSerialization(RocketBatchSpout.class);
        return conf;
	}

	@Override
	public Coordinator getCoordinator(Map conf, TopologyContext context) {
		return new Coordinator();
	}

	@Override
	public Emitter getEmitter(Map conf, TopologyContext context) {
		return new Emitter();
	}
	
	/**
	 * 获取某一个messageQueue
	 * @param mqIdx queue的索引
	 * @return
	 */
    private MessageQueue getMessageQueue(int mqIdx) {   
    	List<MessageQueue> mqL=RocketMQConsumerManager.getMessageQueue(topic);
    	if(mqL!=null&&mqL.size()>mqIdx&&mqIdx>=0){
    		return mqL.get(mqIdx);
    	}
    	return null;
    }

}
