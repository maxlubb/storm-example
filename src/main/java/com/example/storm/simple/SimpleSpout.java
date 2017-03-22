package com.example.storm.simple;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;


/**
 * Created by lubinbin on 15/12/30.
 *
 */
public class SimpleSpout extends BaseRichSpout {

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
