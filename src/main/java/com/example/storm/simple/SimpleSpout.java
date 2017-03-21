package com.example.storm.simple;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * Created by lubinbin on 15/12/30.
 *
 */
public class SimpleSpout extends BaseRichSpout {

    private final static Logger logger = LoggerFactory.getLogger(SimpleSpout.class);
    private SpoutOutputCollector collector = null;

    private int lineOffset = 0;

    private String[] messages = null;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        messages = new String[]{
                "This is a test line, first line.",
                "This is a test line, second line.",
                "This is a test line, third line.",
        };
        this.collector = collector;

    }

    public void nextTuple() {
        int offset = (lineOffset++) % messages.length;
        collector.emit(new Values(messages[offset]));
    }
}
