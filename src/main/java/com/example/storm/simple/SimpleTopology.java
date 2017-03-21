package com.example.storm.simple;

import com.example.storm.OutputBolt;
import com.example.storm.SimpleBolt;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.shade.org.yaml.snakeyaml.Yaml;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;

/**
 * Created by lubinbin on 15/12/29.
 *
 * 基本的storm例子
 */
public class SimpleTopology {

    public static void main(String[] args) throws FileNotFoundException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        SimpleTopology topology = new SimpleTopology();
        topology.submitTopology(args[0]);
    }

    private void submitTopology(String configFile) throws FileNotFoundException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
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

        if (isLocal) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config, builder.createTopology());
        } else {
            StormSubmitter.submitTopology(topologyName, config, builder.createTopology());
        }
    }
}
