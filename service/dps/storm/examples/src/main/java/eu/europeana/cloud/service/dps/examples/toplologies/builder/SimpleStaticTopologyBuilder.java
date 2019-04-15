package eu.europeana.cloud.service.dps.examples.toplologies.builder;


import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.io.*;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.MCSReaderSpout;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by Tarek on 3/27/2017.
 */
public class SimpleStaticTopologyBuilder {

    public static StormTopology buildTopology(MCSReaderSpout spout, AbstractDpsBolt mainBolt, String mainBoltName, String ecloudMcsAddress) {
        TopologyBuilder builder = new TopologyBuilder();

        ReadFileBolt retrieveFileBolt = new ReadFileBolt(ecloudMcsAddress);
        WriteRecordBolt writeRecordBolt = new WriteRecordBolt(ecloudMcsAddress);
        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(ecloudMcsAddress);
        AddResultToDataSetBolt addResultToDataSetBolt = new AddResultToDataSetBolt(ecloudMcsAddress);


        // TOPOLOGY STRUCTURE!
        builder.setSpout(TopologyHelper.SPOUT, spout, 1);
        builder.setBolt(TopologyHelper.RETRIEVE_FILE_BOLT, retrieveFileBolt, 1)
                .shuffleGrouping(TopologyHelper.SPOUT);

        builder.setBolt(mainBoltName, mainBolt, 1)
                .shuffleGrouping(TopologyHelper.RETRIEVE_FILE_BOLT);

        builder.setBolt(TopologyHelper.WRITE_RECORD_BOLT, writeRecordBolt, 1).
                shuffleGrouping(mainBoltName);

        builder.setBolt(TopologyHelper.REVISION_WRITER_BOLT, revisionWriterBolt, 1).
                shuffleGrouping(TopologyHelper.WRITE_RECORD_BOLT);

        builder.setBolt(TopologyHelper.WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt, 1)
                .shuffleGrouping(TopologyHelper.REVISION_WRITER_BOLT);

        return builder.createTopology();
    }

}
