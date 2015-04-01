package eu.europeana.cloud.service.dps.storm.textstripping;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.util.Map;

/**
 *
 * @author la4227 <lucas.anastasiou@open.ac.uk>
 */
public class DummyBolt extends BaseRichBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        System.out.println("declaring output fields");
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        System.out.println("preparing dummy bolt");
    }

    @Override
    public void execute(Tuple input) {
        System.out.println("dummy boly received tuple : " + input.toString());
    }

}
