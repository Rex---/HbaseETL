package com.rexsoft.HbaseETL;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class MyTopology {
	
	public static void main(String[] args) throws InterruptedException {

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("hbase-spout", new HbaseSpout());
		builder.setBolt("etl-bolt", new HbaseETLBolt()).shuffleGrouping("hbase-spout");

		Config conf = new Config();
		conf.setDebug(true);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Hbase ETL", conf, builder.createTopology());
	}
}