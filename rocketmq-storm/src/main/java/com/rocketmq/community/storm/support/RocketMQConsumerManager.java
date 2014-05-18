package com.rocketmq.community.storm.support;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

public class RocketMQConsumerManager {

	private static Map<String,DefaultMQPullConsumer> map=new ConcurrentHashMap<String,DefaultMQPullConsumer>();
	private static Map<String,List<MessageQueue>> mqMap=new ConcurrentHashMap<String,List<MessageQueue>>();
	
	
	public static void registConsumer(final String topic,String hosts,String consumerGroup) throws MQClientException{
		DefaultMQPullConsumer consumer=new DefaultMQPullConsumer(consumerGroup);
		consumer.setNamesrvAddr(hosts);
		consumer.setMessageModel(MessageModel.CLUSTERING);
		consumer.setRegisterTopics(new HashSet<String>(){
			{
				add(topic);
			}
		});
		consumer.start();
		map.put(topic, consumer);
	}
	
	
	public static DefaultMQPullConsumer getConsumer(String topic){
		return map.get(topic);
	}
	
	public static List<MessageQueue> getMessageQueue(String topic){
		DefaultMQPullConsumer consumer=map.get(topic);
		if(consumer!=null){
			List mqList=mqMap.get(topic);
			if(mqList==null){
				try {
					Set<MessageQueue> mqs=consumer.fetchSubscribeMessageQueues(topic);
					mqList=new ArrayList<MessageQueue>(mqs);
					mqMap.put(topic, mqList);
					return mqList;
				} catch (MQClientException e) {
					e.printStackTrace();
				}
			}else{
				return mqList;
			}
		}
		return null;
	}
}
