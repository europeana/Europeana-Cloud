package eu.europeana.cloud.service.dps.storm.utils;


import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * @author akrystian
 */
public class TestInspectionBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 1L;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(NotificationTuple.getFields());
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        collector.emit(input.getValues());
    }
}
