package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
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

  @Override
  protected void emitTuple(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
  }
}
