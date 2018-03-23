package eu.europeana.cloud.service.dps.examples.toplologies.builder;

import eu.europeana.cloud.service.dps.examples.StaticDpsTaskSpout;
import eu.europeana.cloud.service.dps.storm.io.AddResultToDataSetBolt;
import eu.europeana.cloud.service.dps.storm.io.OAIWriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.IdentifiersHarvestingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.RecordHarvestingBolt;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.TaskSplittingBolt;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by Tarek on 3/27/2017.
 */
public class SimpleStaticOAITopologyBuilder {
    private static final long DEFAULT_INTERVAL = 2592000000l;

    public static StormTopology buildTopology(StaticDpsTaskSpout spout, String uisAddress, String ecloudMcsAddress) {
        TopologyBuilder builder = new TopologyBuilder();

        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(ecloudMcsAddress);
        AddResultToDataSetBolt addResultToDataSetBolt = new AddResultToDataSetBolt(ecloudMcsAddress);


        // TOPOLOGY STRUCTURE!
        builder.setSpout(TopologyHelper.SPOUT, spout, 1);


        builder.setBolt(TopologyHelper.TASK_SPLITTING_BOLT, new TaskSplittingBolt(DEFAULT_INTERVAL), 1)
                .shuffleGrouping(TopologyHelper.SPOUT);

        builder.setBolt(TopologyHelper.IDENTIFIERS_HARVESTING_BOLT, new IdentifiersHarvestingBolt(), 1)
                .shuffleGrouping(TopologyHelper.TASK_SPLITTING_BOLT);

        builder.setBolt(TopologyHelper.RECORD_HARVESTING_BOLT, new RecordHarvestingBolt(), 1)
                .shuffleGrouping(TopologyHelper.IDENTIFIERS_HARVESTING_BOLT);


        builder.setBolt(TopologyHelper.WRITE_RECORD_BOLT, new OAIWriteRecordBolt(ecloudMcsAddress, uisAddress), 1).
                shuffleGrouping(TopologyHelper.RECORD_HARVESTING_BOLT);

        builder.setBolt(TopologyHelper.REVISION_WRITER_BOLT, revisionWriterBolt, 1).
                shuffleGrouping(TopologyHelper.WRITE_RECORD_BOLT);

        builder.setBolt(TopologyHelper.WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt, 1)
                .shuffleGrouping(TopologyHelper.REVISION_WRITER_BOLT);

        return builder.createTopology();
    }

}
