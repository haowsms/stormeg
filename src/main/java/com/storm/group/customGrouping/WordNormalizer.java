package com.storm.group.customGrouping;

import java.util.List;
import java.util.Map;
import java.util.Random;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordNormalizer extends BaseBasicBolt {
	private List<Integer> numCounterTasks;

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		  this.numCounterTasks = context.getComponentTasks("word-counter-direct");
			System.out.println("prepare 'word-counter-direct' task=" + this.numCounterTasks);
	}
	
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
				int index = this.numCounterTasks.get(getWordCountIndex(word));
				System.out.println("WordNormalizer�� �յ�һ�����ݣ����������ǣ� " + sentence + "�������ڱ��и�и�����ĵ����� " + word + "  ���͵�index=" + index);
//				collector.emit(new Values(word, "new1>>>" + System.currentTimeMillis() + ":" + Math.random()));
				collector.emitDirect(index,new Values(word, "new1>>>" + System.currentTimeMillis() + ":" + Math.random()));
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
	
	public Integer getWordCountIndex(String word) {
        word = word.trim().toUpperCase();
        if(word.isEmpty()){
            return 0;
        }else{
            return word.charAt(0) % numCounterTasks.size();
        }
    }
}
