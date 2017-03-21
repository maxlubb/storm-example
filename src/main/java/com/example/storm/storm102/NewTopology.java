package com.example.storm.storm102;

import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by lubinbin on 17/2/14.
 */
public class NewTopology {

    public static void main(String[] args) {
        String zkHosts="10.140.60.124:2181"; // kafka集群的zk
        String topic = "client-all-log";
        String zkRoot="/example/kafkaspout"; // zkRoot与spoutId组合,构成了kafkaspout在storm zk中存储信息的路径,主要存储kafka的消费信息
        String spoutId = "kafkaspout";
        SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts(zkHosts),topic,zkRoot,spoutId);
        TopologyBuilder builder = new TopologyBuilder();

    }
}
