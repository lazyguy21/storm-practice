package com.huizhuang.logAnalysis;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.google.common.collect.Maps;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.util.Map;

/**
 * Created by ye on 15-12-4.
 */
public class MyKafkaTopology {
    public static void main(String[] args) {
        IRichSpout kafkaSpout = assembleKafkaSpout();

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("KafkaSpout", kafkaSpout);

        topologyBuilder.setBolt("MachineIdGroupBolt", new MachineIdGroupBolt()).allGrouping("KafkaSpout");
        topologyBuilder.setBolt("PVTimeOfMachineBolt", new PVTimeOfMachineBolt(), 2).fieldsGrouping("MachineIdGroupBolt", new Fields("machineid"));
        topologyBuilder.setBolt("Count2TByObjectIdBolt", new Count2TByObjectIdBolt(), 2).fieldsGrouping("PVTimeOfMachineBolt", new Fields("objectid"));
        topologyBuilder.setBolt("JdbcInsertBolt", assembleJdbcInsertBolt()).allGrouping("Count2TByObjectIdBolt");


        StormTopology topology = topologyBuilder.createTopology();

        LocalCluster localCluster = new LocalCluster();
        Config conf = new Config();

        // Submit topology for execution
        localCluster.submitTopology("KafkaToplogy", conf, topology);

        try {
            // Wait for some time before exiting
            System.out.println("Waiting to consume from kafka");
            Thread.sleep(110000);
        } catch (Exception exception) {
            System.out.println("Thread interrupted exception : " + exception);
        }

        // kill the KafkaTopology
        localCluster.killTopology("KafkaToplogy");

        // shut down the storm test cluster
        localCluster.shutdown();
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
    private static IRichBolt assembleJdbcInsertBolt() {
        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://192.168.10.83:3306/hz_report");
        hikariConfigMap.put("dataSource.user","dev");
        hikariConfigMap.put("dataSource.password","redhat");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);
        String tableName = "test_pvcv";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);
        JdbcInsertBolt jdbcInsertBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper);
        jdbcInsertBolt.withTableName("test_pvcv");
        return jdbcInsertBolt;
    }
}
