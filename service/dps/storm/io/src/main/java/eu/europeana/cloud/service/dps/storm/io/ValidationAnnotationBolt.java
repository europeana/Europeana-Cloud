package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.RepresentationVersionAnnotation;
import eu.europeana.cloud.common.model.RepresentationVersionAnnotation.AnnotationKey;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import org.apache.storm.tuple.Tuple;

/**
 * Validation needs ValidationAnnotationBolt implementation which sens result to normal stream and not notification,
 * cause it is not the last bolt in the validation topology.
 */
public class ValidationAnnotationBolt extends AbstractAnnotationBolt{

  public ValidationAnnotationBolt(CassandraProperties cassandraProperties,
      String ecloudMcsAddress, String topologyUserName, String topologyUserPassword) {
    super(cassandraProperties, ecloudMcsAddress, topologyUserName, topologyUserPassword);
  }

  @Override
  protected RepresentationVersionAnnotation createAnnotation(CommonTaskTuple tuple) {
    return new RepresentationVersionAnnotation(AnnotationKey.VALID, null);
  }

  protected void emitSuccessfulResult(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {
    outputCollector.emit(anchorTuple, commonTaskTuple.toStormTuple());
    outputCollector.ack(anchorTuple);
  }
}
