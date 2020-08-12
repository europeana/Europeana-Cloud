package eu.europeana.cloud.service.dps.examples.toplologies.builder;

import eu.europeana.cloud.http.spout.HttpKafkaSpout;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.io.AddResultToDataSetBolt;
import eu.europeana.cloud.service.dps.storm.io.HarvestingWriteRecordBolt;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.utils.TopologyHelper;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.*;
import static eu.europeana.cloud.service.dps.storm.utils.TopologyHelper.*;

/**
 * Created by Tarek on 3/27/2017.
 */
public class SimpleStaticHTTPTopologyBuilder {


    public static StormTopology buildTopology(HttpKafkaSpout spout, String uisAddress, String ecloudMcsAddress) {
        TopologyBuilder builder = new TopologyBuilder();

        RevisionWriterBolt revisionWriterBolt = new RevisionWriterBolt(ecloudMcsAddress);
        AddResultToDataSetBolt addResultToDataSetBolt = new AddResultToDataSetBolt(ecloudMcsAddress);

        // TOPOLOGY STRUCTURE!
        builder.setSpout(TopologyHelper.SPOUT, spout, 1);

        builder.setBolt(TopologyHelper.WRITE_RECORD_BOLT, new HarvestingWriteRecordBolt(ecloudMcsAddress,uisAddress), 1)
                .shuffleGrouping(TopologyHelper.SPOUT);

        builder.setBolt(TopologyHelper.REVISION_WRITER_BOLT, revisionWriterBolt, 1).
                shuffleGrouping(TopologyHelper.WRITE_RECORD_BOLT);

        builder.setBolt(TopologyHelper.WRITE_TO_DATA_SET_BOLT, addResultToDataSetBolt, 1)
                .shuffleGrouping(TopologyHelper.REVISION_WRITER_BOLT);

        builder.setBolt(NOTIFICATION_BOLT, new NotificationBolt(
                DEFAULT_CASSANDRA_HOSTS,
                Integer.parseInt(DEFAULT_CASSANDRA_PORT),
                DEFAULT_CASSANDRA_KEYSPACE_NAME,
                DEFAULT_CASSANDRA_USERNAME,
                DEFAULT_CASSANDRA_SECRET_TOKEN),
                1
        )
                .fieldsGrouping(WRITE_RECORD_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(REVISION_WRITER_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName))
                .fieldsGrouping(WRITE_TO_DATA_SET_BOLT, AbstractDpsBolt.NOTIFICATION_STREAM_NAME,
                        new Fields(NotificationTuple.taskIdFieldName));

        return builder.createTopology();
    }

}
