package eu.europeana.cloud.service.dps.storm.bolts;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class TestBolt extends BaseBasicBolt {
    public static final Logger LOG = LoggerFactory.getLogger(TestBolt.class);
	
    private static final long serialVersionUID = 1L;

	@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        LOG.info(tuple.toString());
    }
}
