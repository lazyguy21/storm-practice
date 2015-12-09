package com.huizhuang.logAnalysis;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

/**
 * Created by ye on 15-12-4.
 */
public class LogAnalysisClusterTopology {
    public static void main(String[] args) {
        IRichSpout kafkaSpout = assembleKafkaSpout();

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("KafkaSpout", kafkaSpout);
        topologyBuilder.setBolt("MachineIdGroupBolt", new MachineIdGroupBolt()).allGrouping("KafkaSpout");
        topologyBuilder.setBolt("PVTimeOfMachineBolt", new PVTimeOfMachineBolt(), 2).fieldsGrouping("MachineIdGroupBolt", new Fields("machineid"));
        topologyBuilder.setBolt("Count2TByObjectIdBolt", new Count2TByObjectIdBolt(), 2).fieldsGrouping("PVTimeOfMachineBolt", new Fields("objectid"));
        StormTopology topology = topologyBuilder.createTopology();

//        LocalCluster localCluster = new LocalCluster();
        Config conf = new Config();

        conf.setNumWorkers(3);

        try {
            // args[0] is the name of submitted topology
            StormSubmitter.submitTopology(args[0], conf, topology);
        } catch (AlreadyAliveException alreadyAliveException) {
            System.out.println(alreadyAliveException);
        } catch (InvalidTopologyException invalidTopologyException) {
            System.out.println(invalidTopologyException);
        }
    }

    private static IRichSpout assembleKafkaSpout() {
        ZkHosts zkHosts = new ZkHosts("192.168.1.130:2181");
        SchemeAsMultiScheme stringScheme = new SchemeAsMultiScheme(new StringScheme());
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "testPVCV2", "/testPVCV2", "0");
        kafkaConfig.scheme = stringScheme;
        // We want to consume all the first messages in the topic everytime
        // we run the topology to help in debugging. In production, this
        // property should be false
        kafkaConfig.forceFromStart = true;

        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        return kafkaSpout;
    }
}
