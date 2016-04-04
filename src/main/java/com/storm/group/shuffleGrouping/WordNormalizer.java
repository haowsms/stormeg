package com.storm.group.shuffleGrouping;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordNormalizer extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public void cleanup() {
		System.out.println("��һ���ı��и�ɵ��ʣ�����װcollector�з����ȥ ---��ϣ�");
	}

	/**
	 * ���ܵĲ�����WordReader�����ľ��ӣ���input�������Ǿ��� execute�������������и��γɵĵ��ʷ���
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println("WordNormalizer-execute object: " + collector + " this=" + this);
		String sentence = input.getString(0);
		String[] words = sentence.split(" ");
		// System.out.println("WordNormalizer�� �յ�һ�����ݣ����������ǣ� "+ sentence);
		for (String word : words) {
			word = word.trim();
			if (!word.isEmpty()) {
				word = word.toLowerCase();
				System.out.println("WordNormalizer�� �յ�һ�����ݣ����������ǣ� " + sentence + "�������ڱ��и�и�����ĵ����� " + word);
			}
		}
	}

	/**
	 * �����ֶ�id����id�ڼ�ģʽ��û���ô������ڰ����ֶη����ģʽ���кܴ���ô���
	 * ��declarer�����кܴ����ã����ǻ����Ե���declarer.declareStream();������stramId��
	 * ��id��������������Ӹ��ӵ������˽ṹ
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "new"));
	}
}
