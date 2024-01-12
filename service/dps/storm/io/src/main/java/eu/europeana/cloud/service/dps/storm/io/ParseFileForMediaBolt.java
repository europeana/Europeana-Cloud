package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.throttling.ThrottlingTupleGroupSelector;
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
  protected StormTaskTuple createStormTuple(StormTaskTuple stormTaskTuple, RdfResourceEntry rdfResourceEntry, int linksCount) {
    StormTaskTuple tuple = super.createStormTuple(stormTaskTuple, rdfResourceEntry, linksCount);
    tuple.addParameter(PluginParameterKeys.MAIN_THUMBNAIL_AVAILABLE,
        stormTaskTuple.getParameter(PluginParameterKeys.MAIN_THUMBNAIL_AVAILABLE));
    applyThrottling(tuple);
    return tuple;
  }

  @Override
  protected int getLinksCount(StormTaskTuple tuple, int resourcesCount) {
    return Integer.parseInt(tuple.getParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT));
  }

  private void applyThrottling(StormTaskTuple tuple) {
    tuple.setThrottlingGroupingAttribute(generator.generateForResourceProcessingBolt(tuple));
  }

  @Override
  public void prepare() {
    super.prepare();
    generator = new ThrottlingTupleGroupSelector();
  }
}
