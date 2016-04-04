package com.storm.group.allGrouping;

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

	// storm�ڼ�⵽һ��tuple������topology�ɹ������ʱ�����ack���������fail��
	public void ack(Object msgId) {
		System.out.println("OK:" + msgId);
	}

	public void close() {
	}

	// storm�ڼ�⵽һ��tuple������topology�ɹ������ʱ�����ack���������fail��
	public void fail(Object msgId) {
		System.out.println("FAIL:" + msgId);
	}

	/*
	 * ��SpoutTracker���б����ã�ÿ����һ�ξͿ�����storm��Ⱥ�з���һ�����ݣ�һ��tupleԪ�飩���÷����ᱻ��ͣ�ĵ���
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
				System.out.println("WordReader�� ��ȡ��һ�����ݣ�" + str);
				this.collector.emit(new Values(str), str);
				System.out.println("WordReader�� ������һ�����ݣ�" + str);
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
	 * �����ֶ�id����id�ڼ�ģʽ��û���ô������ڰ����ֶη����ģʽ���кܴ���ô���
	 * ��declarer�����кܴ����ã����ǻ����Ե���declarer.declareStream();������stramId��
	 * ��id��������������Ӹ��ӵ������˽ṹ
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
}
