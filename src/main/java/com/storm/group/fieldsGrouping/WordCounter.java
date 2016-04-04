package com.storm.group.fieldsGrouping;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class WordCounter extends BaseBasicBolt {

	private static final long serialVersionUID = 5678586644899822142L;
	Integer id;
	String name;
	// ����Map��װ���Ľ��
	Map<String, Integer> counters;

	/**
	 * ��spout����ʱ�����ã������Ľ����ʾ����
	 * 
	 * �Y��: -- Word Counter [word-counter-2] -- really: 1 but: 1 application: 1
	 * is: 2 great: 2
	 */
	@Override
	public void cleanup() {
		System.out.println("-- Word Counter [" + name + "-" + id + "] --");
		for (Map.Entry<String, Integer> entry : counters.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue());
		}
		System.out.println("ʵ�ּ������Ĺ��� --�ꮅ��");
	}

	/**
	 * ��ʼ������
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.counters = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	/**
	 * ʵ�ּ������Ĺ��ܣ���һ�ν�collector�е�Ԫ�ش���ڳ�Ա����counters��Map����.
	 * ���counters��Map�����Ѿ����ڸ�Ԫ�أ�getValule����Value�����ۼӲ�����
	 */
	public void execute(Tuple input, BasicOutputCollector collector) {
		System.out.println("WordCounter-execute object: " + collector + " this=" + this);
		String str = input.getString(0);
		System.out.println("-----------" + input.getString(1));
		System.out.println("WordCounter �������յ����� " + str);
		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
	}
}
