package com.homeappzz.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class SentenceSpout extends BaseRichSpout {

    private ConcurrentHashMap<UUID, Values> pendingTuples;
    private SpoutOutputCollector outputCollector;
    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i don't think i like fleas"
    };
    private int index = 0;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.outputCollector = collector;
        this.pendingTuples = new ConcurrentHashMap<UUID, Values>();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }

    @Override
    public void nextTuple() {
        UUID msgId = UUID.randomUUID();
        Values value = new Values(sentences[index]);
        this.pendingTuples.put(msgId, value);
        this.outputCollector.emit(value, msgId);
        index++;
        if (index >= sentences.length) {
            index = 0;
        }
        Utils.sleep(1);
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
        this.pendingTuples.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        this.outputCollector.emit(this.pendingTuples.get(msgId), msgId);
    }
}
