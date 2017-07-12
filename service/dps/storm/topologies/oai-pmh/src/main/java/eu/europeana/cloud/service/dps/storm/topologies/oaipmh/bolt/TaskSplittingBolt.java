package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.Splitter;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.common.OAIHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Split Task to tuple per schema by set by range of dates.
 * The bolt will call an appropriate number of HarvestingBolts based on
 * schema(s), set(s), timeSpan
 * iT should accept an input with a task to harvest a period of time, divide this period into smaller pieces
 * based on date or number of parts - Max One Month) and for each one of them call Harvesting Bolt
 */

public class TaskSplittingBolt extends AbstractDpsBolt {
    public static final Logger LOGGER = LoggerFactory.getLogger(TaskSplittingBolt.class);
    private long defaultInterval;


    //will be passed on the topology level as it will be extracted as a property of oai-topology-config file
    public TaskSplittingBolt(long defaultInterval) {
        this.defaultInterval = defaultInterval;
    }

    public void execute(StormTaskTuple stormTaskTuple) {
        try {
            Splitter splitter = new Splitter(stormTaskTuple, inputTuple, outputCollector, new OAIHelper(stormTaskTuple.getFileUrl()), defaultInterval);
            splitter.splitBySchema();

        } catch (Exception e) {
            LOGGER.error("Task Splitting Bolt error: {} ", e.getMessage());
        }
    }

    @Override
    public void prepare() {
    }


}
