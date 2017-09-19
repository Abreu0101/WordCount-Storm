package com.homeappzz.bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ReportBolt extends BaseRichBolt {

    private HashMap<String, Long> wordCounts;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.wordCounts = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        Long count = input.getLongByField("count");
        this.wordCounts.put(word, count);
    }

    @Override
    public void cleanup() {
        super.cleanup();
        System.out.println("--- Report Output ---");
        ArrayList<String> wordsKeys = new ArrayList<String>(wordCounts.keySet());
        Collections.sort(wordsKeys);
        for (String wordKey : wordsKeys) {
            System.out.println("Word : " + wordKey + " " + "Count : " + this.wordCounts.get(wordKey));
        }
        System.out.print("--- End Report ---");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
