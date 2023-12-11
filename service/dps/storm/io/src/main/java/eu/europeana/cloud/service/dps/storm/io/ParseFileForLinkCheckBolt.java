package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.metis.mediaprocessing.exception.RdfDeserializationException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import java.util.List;

public class ParseFileForLinkCheckBolt extends ParseFileBolt {

  public ParseFileForLinkCheckBolt(CassandraProperties cassandraProperties, String ecloudMcsAddress,
      String ecloudMcsUser, String ecloudMcsUserPassword) {
    super(cassandraProperties, ecloudMcsAddress, ecloudMcsUser, ecloudMcsUserPassword);
  }

  protected List<RdfResourceEntry> getResourcesFromRDF(byte[] bytes) throws RdfDeserializationException {
    // TODO Here we use deprecated method which should be changed to rdfDeserializer.getResourceEntriesForLinkChecking(bytes)
    return rdfDeserializer.getResourceEntriesForMediaExtraction(bytes);
  }

  @Override
  protected int getLinksCount(StormTaskTuple tuple, int resourcesCount) {
    tuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, String.valueOf(resourcesCount));
    return resourcesCount;
  }
}
