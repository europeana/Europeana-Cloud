package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import java.io.Serial;
import org.apache.storm.tuple.Tuple;

/**
 * Validation needs ValidationAddToDatasetBolt implementation which sends result to normal stream and not notification,
 * cause it is not the last bolt in the validation topology.
 */
public class ValidationAddToDatasetBolt extends AddToDatasetBolt{

  @Serial
  private static final long serialVersionUID = 1L;

  /**
   * Creates bolt
   *
   * @param cassandraProperties - properties of Cassandra DPS schema
   * @param ecloudMcsAddress - address of MCS application
   * @param topologyUserName - user for MCS API
   * @param topologyUserPassword - user for MCS API
   */
  public ValidationAddToDatasetBolt(CassandraProperties cassandraProperties,
      String ecloudMcsAddress, String topologyUserName, String topologyUserPassword) {
    super(cassandraProperties, ecloudMcsAddress, topologyUserName, topologyUserPassword);
  }

  @Override
  protected void emitSuccessfulResult(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {
    outputCollector.emit(anchorTuple, commonTaskTuple.toStormTuple());
    outputCollector.ack(anchorTuple);
  }
}
