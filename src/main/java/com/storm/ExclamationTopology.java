package com.storm;

import com.storm.bolt.ExclamationBolt;
import com.storm.spout.TestWordSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class ExclamationTopology {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word", new TestWordSpout(), 1);
		builder.setBolt("exlcaim1", new ExclamationBolt(), 1).shuffleGrouping("word");
		builder.setBolt("exlcaim2", new ExclamationBolt(), 3).shuffleGrouping("exlcaim1");
		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology("test", conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(120000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
}
