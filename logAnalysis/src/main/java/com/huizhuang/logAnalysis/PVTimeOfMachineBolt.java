package com.huizhuang.logAnalysis;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PVTimeOfMachineBolt extends BaseBasicBolt {
    private Map<String, List<Object>> tempLogMap = new HashMap<>();


    public void execute(Tuple input, BasicOutputCollector collector) {
        List<Object> logDataNow = input.getValues();
        String machineIdNow = input.getStringByField("machineid");
        Long createTimeNow = Long.valueOf(input.getStringByField("createtime"));
        Long objectId = Long.valueOf(input.getStringByField("objectid"));

        if (tempLogMap.containsKey(machineIdNow)) {
            //计算上一条相同machineId 的log和现在这条log的时间差，即页面停留时间
            List<Object> logObjectListBefore = tempLogMap.get(machineIdNow);
            Object createTimeObjectBefore = logObjectListBefore.get(1);
            Long createTimeBefore = Long.valueOf((String) createTimeObjectBefore);

            //存现在的log
            tempLogMap.put(machineIdNow, logDataNow);
            //有的日志里面没有取得createTime，不做统计
            if (createTimeNow.equals(0L) || createTimeBefore.equals(0L)) {
                return;
            }

            long millisecondsInternal = createTimeNow - createTimeBefore;
            Long secondsInternal = millisecondsInternal / 1000;
            //过滤条件,大于10min置为0
            secondsInternal = ((secondsInternal / (60 * 10)) >= 10) ? 0 : secondsInternal;


            ArrayList<Object> objects = Lists.newArrayList(logObjectListBefore);
            objects.add(secondsInternal);


            collector.emit(objects);

        } else {
            //存现在的log
            tempLogMap.put(machineIdNow, logDataNow);
        }

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
                "other",
                "secondsInternal"
        ));
    }
}
