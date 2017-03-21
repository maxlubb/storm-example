package com.example.storm.kafka;

import com.example.storm.OutputBolt;
import com.example.storm.SimpleBolt;
import com.example.storm.util.MessageScheme;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.shade.org.yaml.snakeyaml.Yaml;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;

/**
 * Created by lubinbin on 15/12/29.
 *
 * 使用kafkaSpout的例子
 */
public class WithKafkaTopology {

    public static void main(String[] args) throws FileNotFoundException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        WithKafkaTopology topology = new WithKafkaTopology();
        // 提交topology 作业
        topology.submitTopology(args[0]);
    }

    /**
     *
     * @param configFile
     */
    private void submitTopology(String configFile) throws FileNotFoundException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        Yaml yaml = new Yaml();
        Map<String,Object> stormConfig = yaml.loadAs(new FileInputStream(configFile),Map.class);
        boolean isLocal = BooleanUtils.toBoolean(stormConfig.get("localmodel").toString());
        String topologyName = stormConfig.get("topology.name").toString();
        int workers = NumberUtils.toInt(stormConfig.get("workers").toString());
        // kafka spout config
        String zkHosts="10.140.60.124:2181"; // kafka集群的zk
        String topic = "client-all-log";
        String zkRoot="/example/kafkaspout"; // zkRoot与spoutId组合,构成了kafkaspout在storm zk中存储信息的路径,主要存储kafka的消费信息
        String spoutId = "kafkaspout";
        SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts(zkHosts),topic,zkRoot,spoutId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
        //
        // build topology  提交topology
        TopologyBuilder builder = new TopologyBuilder();
        String spoutName= "kafkaSpout";
        String simpleBoltName = "simpleBolt";
        String outputBoltName = "outputBolt";
        // 参数 < spoutName, spout class, 并行度>
        builder.setSpout(spoutName,new KafkaSpout(spoutConfig),1);
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
