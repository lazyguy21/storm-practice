package com.huizhuang.logAnalysis;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class Count2TByObjectIdBolt extends BaseBasicBolt {
    private static final Logger logger = LoggerFactory.getLogger(Count2TByObjectIdBolt.class);
    private Map<String, Long> totalTimeMap = new HashMap<>();
    private Map<String, Long> totalCountMap = new HashMap<>();


    public void execute(Tuple input, BasicOutputCollector collector) {
        String objectId = input.getStringByField("objectid");
        Long secondsInternal = input.getLongByField("secondsInternal");
        if (totalCountMap.containsKey(objectId)) {
            totalCountMap.put(objectId, totalCountMap.get(objectId) + 1);
            totalTimeMap.put(objectId, totalTimeMap.get(objectId) + secondsInternal);
        } else {
            totalCountMap.put(objectId, 1L);
            totalTimeMap.put(objectId, secondsInternal);
        }

        Long amount = totalCountMap.get(objectId);
        Long totalTime = totalTimeMap.get(objectId);
        logger.info("统计: objectid : " + objectId + "  amount : " + amount + " totalTime : " + totalTime);

        ArrayList list = Lists.newArrayList(objectId.toString(), totalTime.toString(), amount.toString());
        collector.emit(list);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("object_id", "total_time", "amount"));
    }
}
