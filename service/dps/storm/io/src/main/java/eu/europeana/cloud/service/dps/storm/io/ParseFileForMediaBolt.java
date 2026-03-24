package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.throttling.ThrottlingTupleGroupSelector;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.metis.mediaprocessing.exception.RdfDeserializationException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;

import java.util.List;

public class ParseFileForMediaBolt extends ParseFileBolt {

  private ThrottlingTupleGroupSelector generator;

  public ParseFileForMediaBolt(CassandraProperties cassandraProperties, String ecloudMcsAddress,
      String ecloudMcsUser, String ecloudMcsUserPassword) {
    super(cassandraProperties, ecloudMcsAddress, ecloudMcsUser, ecloudMcsUserPassword);
  }

  protected List<RdfResourceEntry> getResourcesFromRDF(byte[] bytes) throws RdfDeserializationException {
    return rdfDeserializer.getRemainingResourcesForMediaExtraction(bytes);
  }

  @Override
  protected CommonTaskTuple createStormTuple(CommonTaskTuple commonTaskTuple, RdfResourceEntry rdfResourceEntry, int linksCount) {
    CommonTaskTuple tuple = super.createStormTuple(commonTaskTuple, rdfResourceEntry, linksCount);
    tuple.addParameter(PluginParameterKeys.MAIN_THUMBNAIL_AVAILABLE,
        commonTaskTuple.getParameter(PluginParameterKeys.MAIN_THUMBNAIL_AVAILABLE));
    applyThrottling(tuple);
    return tuple;
  }

  @Override
  protected int getLinksCount(CommonTaskTuple tuple, int resourcesCount) {
    return Integer.parseInt(tuple.getParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT));
  }

  private void applyThrottling(CommonTaskTuple tuple) {
    tuple.setThrottlingGroupingAttribute(generator.generateForResourceProcessingBolt(tuple));
  }

  @Override
  public void prepare() {
    super.prepare();
    generator = new ThrottlingTupleGroupSelector();
  }
}
