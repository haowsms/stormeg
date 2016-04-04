package com.storm.group.customGrouping;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class Topology {
	public static void main(String[] args) throws InterruptedException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-reader", new WordReader());
		builder.setBolt("word-normalizer", new WordNormalizer(), 2).customGrouping(
				"word-reader", new ModuleGrouping());
		builder.setBolt("word-counter-direct", new WordCounter(), 2).directGrouping("word-normalizer");
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);
		conf.put("wordsFile", "E:/archive_workspace/stormeg/file/word.txt");
		conf.setDebug(false);
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("wordCounterTopology", conf, builder.createTopology());
		Thread.sleep(10000);
		cluster.killTopology("wordCounterTopology");
		cluster.shutdown();
	}
}
