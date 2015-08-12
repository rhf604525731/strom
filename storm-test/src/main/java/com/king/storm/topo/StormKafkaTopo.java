package com.king.storm.topo;

import java.util.HashMap;
import java.util.Map;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.king.storm.bolt.SenqueceBolt;
import com.king.storm.deser.MessageScheme;

public class StormKafkaTopo {   
    public static void main(String[] args) throws Exception { 
    	// 配置Zookeeper地址
        BrokerHosts brokerHosts = new ZkHosts("172.16.65.25:2181,172.16.65.26:2181,172.16.65.27:2181");
        // 配置Kafka订阅的Topic，以及zookeeper中数据节点目录和名字
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "topic1", "/zkkafkaspout" , "kafkaspout");
       // 配置KafkaBolt中的kafka.broker.properties
        Config conf = new Config();  
        Map<String, String> map = new HashMap<String, String>(); 
        // 配置Kafka broker地址       
        map.put("metadata.broker.list", "172.16.65.25:9092,172.16.65.26:9092,172.16.65.27:9092");
        // serializer.class为消息的序列化类
        map.put("serializer.class", "kafka.serializer.StringEncoder");
        conf.put("kafka.broker.properties", map);
        // 配置KafkaBolt生成的topic
        conf.put("topic", "topic2");
        
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());  
        TopologyBuilder builder = new TopologyBuilder();   
        builder.setSpout("spout", new KafkaSpout(spoutConfig));  
        builder.setBolt("bolt", new SenqueceBolt()).shuffleGrouping("spout"); 
        builder.setBolt("kafkabolt", new KafkaBolt<String, Integer>()).shuffleGrouping("bolt");        

        if (args != null && args.length > 0) {  
            conf.setNumWorkers(3);  
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());  
        } else {  
            LocalCluster cluster = new LocalCluster();  
            cluster.submitTopology("Topo", conf, builder.createTopology());  
            Utils.sleep(100000);  
            cluster.killTopology("Topo");  
            cluster.shutdown();  
        }  
    }  
}