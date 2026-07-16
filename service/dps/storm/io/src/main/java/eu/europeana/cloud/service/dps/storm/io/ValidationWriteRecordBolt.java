package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import org.apache.storm.tuple.Tuple;

/**
 * Validation needs WriteRecordBolt implmentation which sens result to normal stream and not notification,
 * cause it is not the last bolt in the validation topology.
 */
public class ValidationWriteRecordBolt extends WriteRecordBolt{

  public ValidationWriteRecordBolt(CassandraProperties cassandraProperties,
      String ecloudMcsAddress, String topologyUserName, String topologyUserPassword, boolean topologyCreatingNewData) {
    super(cassandraProperties, ecloudMcsAddress, topologyUserName, topologyUserPassword, topologyCreatingNewData);
  }

  protected void emitSuccessfulResult(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {
    outputCollector.emit(anchorTuple, commonTaskTuple.toStormTuple());
    outputCollector.ack(anchorTuple);
  }
}
