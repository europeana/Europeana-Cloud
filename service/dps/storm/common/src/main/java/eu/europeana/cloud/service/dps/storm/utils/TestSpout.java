package eu.europeana.cloud.service.dps.storm.utils;


import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class TestSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(StormTaskTuple.getFields());
    }


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
    }


    @Override
    public void nextTuple() {
        Utils.sleep(100);

        StormTaskTuple stormTaskTuple = new StormTaskTuple(
                1,
               "taskName",
                null, null, null, new Revision(), new OAIPMHHarvestingDetails());

        collector.emit(stormTaskTuple.toStormTuple());

    }

    @Override
    public void close() {
        super.close();
    }

}




