package com.rexsoft.HbaseETL;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class HbaseSpout extends BaseRichSpout{

	private static final long serialVersionUID = 1L;
	
	private SpoutOutputCollector collector;
	
	private static Configuration configuration;
	
	private static final String tableName = "HbaseETL";
	
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
		this.collector = collector;
		
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "192.168.178.130,192.168.178.131,192.168.178.132");
		configuration.set("hbase.zookeeper.property.clientPort", "2181"); 
	}

	/** 处理所有新插入的，flag为0数据 */
	public void nextTuple() {
		try {
			HTable table = new HTable(configuration, tableName);
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes("Content"), Bytes.toBytes("flag"), CompareOp.EQUAL, Bytes.toBytes("0")); 
			Scan s = new Scan();
			s.setFilter(filter);
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				System.out.println("Spout --> 获得到rowkey:" + new String(r.getRow()));
				for (KeyValue keyValue : r.raw()) {
					if(new String(keyValue.getQualifier()).equalsIgnoreCase("word")){
						collector.emit(new Values(new String(r.getRow()),new String(keyValue.getValue())));
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("key","word"));
	}
}