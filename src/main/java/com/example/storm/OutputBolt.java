package com.example.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lubinbin on 15/12/29.
 * 模仿输出bolt,真实场景更多的是使用可持久化存储或消息队列等作为输出目标
 */
public class OutputBolt extends BaseBasicBolt{

    private static Logger logger = LoggerFactory.getLogger(OutputBolt.class);

    //本地缓存
    private Map<String,Long> counterMap = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        counterMap = new HashMap<String, Long>();
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        String key = input.getStringByField("key");
        long value = input.getLongByField("value");
        if (counterMap.get(key)==null){
            counterMap.put(key,value);
        }else{
            counterMap.put(key,counterMap.get(key) + value);
        }
        // TODO: 15/12/30 极端情况,size可能始终小于预先设置的阈值(假设 阈值为 Long.MAX_VALUE,那么其实词量是达不到的)
        // ,可以使用定时来避免这种情况
        while(counterMap.size() > 5){
            persistData();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        super.cleanup();
        persistData();
    }

    private void persistData(){
        try{
            // 输出到文件 tricky
            logger.info(counterMap.toString());
            // 作为测试,这里只是简单的清空
            counterMap.clear();
        }catch(Exception e){
            logger.error(e.getMessage(),e);
            // TODO: 15/12/29 持久化异常的处理
        }
    }
}
