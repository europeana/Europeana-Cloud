package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.enums.ValidationTypes;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

/**
 * Revision writer bolt used for OAI and HTTP topology. It just emits tuple to next bolt (DuplicatedRecordsProcessorBolt);
 */
public class RevisionWriterBoltForValidation extends RevisionWriterBolt {

  private static final long serialVersionUID = 1L;
  public static final String DUPLICATE_RECORD_STREAM = "DuplicatedRecordRevisionWriterBoltStream";

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    super.declareOutputFields(declarer);

    declarer.declareStream(DUPLICATE_RECORD_STREAM, StormTaskTuple.getFields());
  }

  public RevisionWriterBoltForValidation(CassandraProperties cassandraProperties, String ecloudMcsAddress,
                                         String ecloudMcsUser, String ecloudMcsUserPassword) {
    super(cassandraProperties, ecloudMcsAddress, ecloudMcsUser, ecloudMcsUserPassword);
  }

  private static boolean detectDuplicates(StormTaskTuple stormTaskTuple) {
    return stormTaskTuple.ifParametersContainsKey(PluginParameterKeys.TASK_SUBTYPE) &&
            stormTaskTuple.getParameter(PluginParameterKeys.TASK_SUBTYPE)
                    .equals(ValidationTypes.VALIDATE_EXTERNAL.toString());
  }

  @Override
  protected void emitTuple(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    if (detectDuplicates(stormTaskTuple)) {
      outputCollector.emit(DUPLICATE_RECORD_STREAM, anchorTuple, stormTaskTuple.toStormTuple());
    } else {
      emitSuccessNotification(anchorTuple, stormTaskTuple, "", "");
    }
  }
}
