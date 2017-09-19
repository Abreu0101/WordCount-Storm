package com.homeappzz.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class WordCountBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap<String, Long> wordCount;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.wordCount = new HashMap<String, Long>();
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        this.wordCount.putIfAbsent(word, (long) 0);
        Long counter = this.wordCount.get(word) + 1;
        this.wordCount.put(word, counter);
        this.collector.emit(new Values(word, counter));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }

}
