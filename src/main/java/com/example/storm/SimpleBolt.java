package com.example.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by lubinbin on 15/12/29.
 * 简单的bolt,将从spout收到的消息分词后emit
 */
public class SimpleBolt extends BaseBasicBolt {

    private final static Logger logger = LoggerFactory.getLogger(SimpleBolt.class);
    private StringTokenizer tokenizer;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        // 在bolt第一次加载的时候运行,可以在此做一些初始化操作
        super.prepare(stormConf, context);
        logger.info("init here ");

    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        try{
            logger.debug("in execute");
            String msg = input.getString(0);
            StringTokenizer tokenizer = new StringTokenizer(msg);
            // emit message to follow bolt
            while(tokenizer.hasMoreElements()){
                collector.emit(new Values(tokenizer.nextElement(),1L));
            }
        }catch(Exception e){
            // 保证storm执行过程中不会被异常中断
            logger.error(e.getMessage(),e);
            // 上报,使storm感知到错误,但不会导致计算流中断
            collector.reportError(e);
        }finally{
            logger.debug("out execute");
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key","value"));
    }

    @Override
    public void cleanup() {
        // 在bolt被销毁时 调用,最多的场景是在topology停止时触发该方法被调用
        super.cleanup();
        // TODO: 15/12/29 clean resource or release memory
    }
}
