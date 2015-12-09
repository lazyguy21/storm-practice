package com.huizhuang.logAnalysis;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;

public class MachineIdGroupBolt extends BaseBasicBolt {

	public void execute(Tuple input, BasicOutputCollector collector) {
		// get the sentence from the tuple and print it
		String originalLog = input.getString(0);
		String[] splitLogFields = StringUtils.split(originalLog, "|");
		List logData = Arrays.asList(splitLogFields);
		collector.emit(logData);
		System.out.println("我收到了Log :" + logData);

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(
				"servertime",
				"createtime",
				"type",
				"platform",
				"channel",
				"appid",
				"siteid",
				"objectid",
				"userid",
				"machineid",
				"network",
				"outputip",
				"gpsx",
				"gpsy",
				"seqid",
				"serverid",
				"other"
				));
	}
}
