package com.example.storm.simple;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.jstorm.client.ConfigExtension;
import com.example.storm.OutputBolt;
import com.example.storm.SimpleBolt;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.yaml.snakeyaml.Yaml;
import storm.kafka.KafkaSpout;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;

/**
 * Created by lubinbin on 15/12/29.
 *
 * 基本的storm例子
 */
public class SimpleTopology {

    public static void main(String[] args) throws FileNotFoundException, AlreadyAliveException, InvalidTopologyException {
        SimpleTopology topology = new SimpleTopology();
        topology.submitTopology("/Users/lubinbin/workspace/storm-example/src/main/resources/storm-test.yaml");
    }

    private void submitTopology(String configFile) throws FileNotFoundException, AlreadyAliveException, InvalidTopologyException {
        Yaml yaml = new Yaml();
        Map<String,Object> stormConfig = yaml.loadAs(new FileInputStream(configFile),Map.class);
        boolean isLocal = BooleanUtils.toBoolean(stormConfig.get("localmodel").toString());
        String topologyName = stormConfig.get("topology.name").toString();
        int workers = NumberUtils.toInt(stormConfig.get("workers").toString());

        TopologyBuilder builder = new TopologyBuilder();
        String spoutName= "simpleSpout";
        String simpleBoltName = "simpleBolt";
        String outputBoltName = "outputBolt";
        // 参数 < spoutName, spout class, 并行度>
        builder.setSpout(spoutName,new SimpleSpout(),1);
        builder.setBolt(simpleBoltName,new SimpleBolt(),1).shuffleGrouping(spoutName);
        builder.setBolt(outputBoltName,new OutputBolt(),1).fieldsGrouping(simpleBoltName,new Fields("key"));
        // config storm全局共享的config信息
        Config config = new Config();
        config.putAll(stormConfig);
        config.setNumWorkers(workers);
        ConfigExtension.setUserDefinedLog4jConf(config, "jstorm.log4j.properties");

        if (isLocal) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config, builder.createTopology());
        } else {
            StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
        }
    }
}
