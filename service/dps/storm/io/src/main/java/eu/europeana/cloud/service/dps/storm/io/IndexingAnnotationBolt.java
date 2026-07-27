package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.RepresentationVersionAnnotation;
import eu.europeana.cloud.common.model.RepresentationVersionAnnotation.AnnotationKey;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;

public class IndexingAnnotationBolt extends AbstractAnnotationBolt {

  public IndexingAnnotationBolt(CassandraProperties cassandraProperties,
      String ecloudMcsAddress, String topologyUserName, String topologyUserPassword) {
    super(cassandraProperties, ecloudMcsAddress, topologyUserName, topologyUserPassword);
  }

  @Override
  protected RepresentationVersionAnnotation createAnnotation(CommonTaskTuple tuple) {
    return new RepresentationVersionAnnotation(getDatabase(tuple).getAnnotationKey(), "");
  }

  private TargetIndexingDatabase getDatabase(CommonTaskTuple commonTaskTuple) {
    return TargetIndexingDatabase.valueOf(
        commonTaskTuple.getParameter(PluginParameterKeys.METIS_TARGET_INDEXING_DATABASE));
  }
}
