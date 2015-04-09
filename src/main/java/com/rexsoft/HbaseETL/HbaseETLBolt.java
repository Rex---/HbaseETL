package com.rexsoft.HbaseETL;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class HbaseETLBolt extends BaseRichBolt{

	private static final long serialVersionUID = 1L;
	
	private static Configuration configuration;
	
	private static final String tableName = "HbaseETL";

	public void execute(Tuple tuple) {
		String id = tuple.getString(0);
		String word = tuple.getString(1);
		
	    try {
	    	HTable table = new HTable(configuration, tableName);
		    Put update = new Put(new String(id).getBytes());
		    update.add(new String("Content").getBytes(), new String("word").getBytes(), new String(word+" done ").getBytes());
		    update.add(new String("Content").getBytes(), new String("flag").getBytes(), new String("1").getBytes());  
			table.put(update);
		} catch (RetriesExhaustedWithDetailsException e) {
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "192.168.178.130,192.168.178.131,192.168.178.132");
		configuration.set("hbase.zookeeper.property.clientPort", "2181"); 
	}
	public void declareOutputFields(OutputFieldsDeclarer arg0) {}
}