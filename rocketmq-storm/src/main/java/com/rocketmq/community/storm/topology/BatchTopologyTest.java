package com.rocketmq.community.storm.topology;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.FailedException;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RegisteredGlobalState;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.rocketmq.community.storm.spout.RocketBatchSpout;

public class BatchTopologyTest {
	
	public static class BatchCount extends BaseBatchBolt {
		Object _id;
		BatchOutputCollector _collector;
		
		int _count = 0;
		
		@Override
		public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
		  _collector = collector;
		  _id = id;
		}
		
		@Override
		public void execute(Tuple tuple) {
		  _count++;
		}
		
		@Override
		public void finishBatch() {
		  System.out.println("id:"+_id+" count:"+_count);
		  _collector.emit(new Values(_id, _count));
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
		  declarer.declare(new Fields("id", "count"));
		}
	}
	  
	public static class ShowGlobalCount extends BaseTransactionalBolt implements ICommitter{
		
		TransactionAttempt _attempt;
		BatchOutputCollector _collector;
		
		int _sum = 0;
		
		@Override
		public void prepare(Map conf, TopologyContext context,
				BatchOutputCollector collector, TransactionAttempt id) {
			_attempt=id;
			_collector=collector;
		}
	
		@Override
		public void execute(Tuple tuple) {
			AtomicInteger flag=(AtomicInteger)RegisteredGlobalState.getState("count");
			if(flag.getAndDecrement()>0){
				throw new FailedException();
			}else{
				_sum += tuple.getInteger(1);
			}
		}
	
		@Override
		public void finishBatch() {
			
			System.out.println("===========trans:"+_attempt+"  sum:"+_sum+"====================");
			 _collector.emit(new Values(_attempt,_sum));
		}
	
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			 declarer.declare(new Fields("id", "sum"));			
		}
		  
}

	
	public static void main(String args[]) throws MQClientException{
		String consumerGroup="groupTest";
		AtomicInteger flag=new AtomicInteger(4);
		RegisteredGlobalState.setState("count",flag);
		RocketBatchSpout spout = new RocketBatchSpout(consumerGroup,"rocketTopicTest","127.0.0.1:9876",20,new Fields("message"));
	    TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("global-count", "spout", spout,4);
	    builder.setBolt("partial-count", new BatchCount()).noneGrouping("spout");
	    builder.setBolt("sum", new ShowGlobalCount()).globalGrouping("partial-count");

	    LocalCluster cluster = new LocalCluster();

	    Config config = new Config();
	    config.setDebug(false);
	    config.setMaxSpoutPending(3);

	    cluster.submitTopology("global-count-topology", config, builder.buildTopology());

//	    Thread.sleep(3000);
//	    cluster.shutdown();
	}

}
