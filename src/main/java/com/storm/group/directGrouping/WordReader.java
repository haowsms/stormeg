package com.storm.group.directGrouping;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordReader extends BaseRichSpout {

	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private String filePath;
	private boolean completed = false;

	// storm在检测到一个tuple被整个topology成功处理的时候调用ack，否则调用fail。
	public void ack(Object msgId) {
		System.out.println("OK:" + msgId);
	}

	public void close() {
	}

	// storm在检测到一个tuple被整个topology成功处理的时候调用ack，否则调用fail。
	public void fail(Object msgId) {
		System.out.println("FAIL:" + msgId);
	}

	/*
	 * 在SpoutTracker类中被调用，每调用一次就可以向storm集群中发射一条数据（一个tuple元组），该方法会被不停的调用
	 */
	public void nextTuple() {
		if (completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
			return;
		}
		String str;
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			while ((str = reader.readLine()) != null) {
				System.out.println("WordReader类 读取到一行数据：" + str);
				this.collector.emit(new Values(str), str);
				System.out.println("WordReader类 发射了一条数据：" + str);
				System.out.println("nextTuple-collector object: " + collector + " this=" + this);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading tuple", e);
		} finally {
			completed = true;
		}
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
			this.fileReader = new FileReader(conf.get("wordsFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file [" + conf.get("wordFile") + "]");
		}
		this.filePath = conf.get("wordsFile").toString();
		this.collector = collector;
		System.out.println("open-collector object: " + collector + " this=" + this);
	}

	/**
	 * 定义字段id，该id在简单模式下没有用处，但在按照字段分组的模式下有很大的用处。
	 * 该declarer变量有很大作用，我们还可以调用declarer.declareStream();来定义stramId，
	 * 该id可以用来定义更加复杂的流拓扑结构
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
}
