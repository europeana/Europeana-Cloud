package eu.europeana.cloud.service.dps.storm.textstripping;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author la4227 <lucas.anastasiou@open.ac.uk>
 */
public class TextStrippingBolt extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        System.out.println("hello form eclare output fileds of bolt");
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        System.out.println("inout:" + input.toString());
        System.out.println("0 position : " + input.getString(0));
    }
}
