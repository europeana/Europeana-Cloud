package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.cloud.service.dps.storm.io.RevisionWriterBolt;
import eu.europeana.cloud.service.dps.storm.topologies.validation.topology.enums.ValidationTypes;
import org.apache.storm.tuple.Tuple;

/**
 * Revision writer bolt used for OAI and HTTP topology. It just emits tuple to next bolt (DuplicatedRecordsProcessorBolt);
 */
public class RevisionWriterBoltForValidation extends RevisionWriterBolt {

  private static final long serialVersionUID = 1L;


  public RevisionWriterBoltForValidation(CassandraProperties cassandraProperties, String ecloudMcsAddress,
                                         String ecloudMcsUser, String ecloudMcsUserPassword) {
    super(cassandraProperties, ecloudMcsAddress, ecloudMcsUser, ecloudMcsUserPassword);
  }

  private static boolean detectDuplicates(CommonTaskTuple stormTaskTuple) {
    return stormTaskTuple.ifParametersContainsKey(PluginParameterKeys.JOB_NAME) &&
            stormTaskTuple.getParameter(PluginParameterKeys.JOB_NAME)
                    .equals(ValidationTypes.VALIDATE_EXTERNAL.toString());
  }

  @Override
  protected void emitTuple(Tuple anchorTuple, CommonTaskTuple stormTaskTuple) {
    if (detectDuplicates(stormTaskTuple)) {
      outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
    } else {
      super.emitTuple(anchorTuple,stormTaskTuple);
    }
  }
}
