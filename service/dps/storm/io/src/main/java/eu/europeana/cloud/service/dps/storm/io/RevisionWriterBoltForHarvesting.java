package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.apache.storm.tuple.Tuple;

/**
 * Revision writer bolt used for OAI and HTTP topology. It just emits tuple to next bolt (DuplicatedRecordsProcessorBolt);
 */
public class RevisionWriterBoltForHarvesting extends RevisionWriterBolt {

  private static final long serialVersionUID = 1L;

  public RevisionWriterBoltForHarvesting(String ecloudMcsAddress,
                                         String ecloudMcsUser,
                                         String ecloudMcsUserPassword) {
    super(ecloudMcsAddress, ecloudMcsUser, ecloudMcsUserPassword);
  }

  @Override
  protected void emitTuple(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    outputCollector.emit(anchorTuple, stormTaskTuple.toStormTuple());
  }
}
