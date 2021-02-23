package eu.europeana.cloud.service.dps.storm.topologies.oaipmh;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER;

public class TestingBolt extends AbstractDpsBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(TestingBolt.class);


    @Override
    public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
        LOGGER.info("Starting testing bolt for: {}", stormTaskTuple.getParameter(CLOUD_LOCAL_IDENTIFIER));
        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            new RuntimeException(e);
        }
        outputCollector.ack(anchorTuple);
    }

     @Override
    public void prepare() {

    }


}
