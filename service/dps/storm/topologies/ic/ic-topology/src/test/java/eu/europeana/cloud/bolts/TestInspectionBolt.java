package eu.europeana.cloud.bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;

/**
 * @author akrystian
 */
public class TestInspectionBolt extends BaseBasicBolt {
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(NotificationTuple.getFields());
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        collector.emit(input.getValues());
    }
}
