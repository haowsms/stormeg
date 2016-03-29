package com.storm.spout;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TestWordSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;

	@Override
	public void nextTuple() {
		Utils.sleep(100);
		final String[] words = new String[]{"nathan", "mike", "jackson", "golda", "bertels"};
		final Random rand = new Random();
		String word = words[rand.nextInt(words.length)];
		_collector.emit(new Values(word));

	}

	@Override
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		_collector = arg2;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		arg0.declare(new Fields("word"));
	}

}
