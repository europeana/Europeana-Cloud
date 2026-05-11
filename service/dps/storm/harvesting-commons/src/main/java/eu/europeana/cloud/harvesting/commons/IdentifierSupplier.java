package eu.europeana.cloud.harvesting.commons;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;

public class IdentifierSupplier {

  public void prepareIdentifiers(CommonTaskTuple tuple) throws EuropeanaIdException {

    String metisDatasetId = tuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
    if (StringUtils.isEmpty(metisDatasetId)) {
      throw new EuropeanaIdException(
          "Could not create identifier - parameter " + PluginParameterKeys.METIS_DATASET_ID + " is empty!");
    }

    EuropeanaGeneratedIdsMap europeanaIdentifier = getEuropeanaIdentifier(tuple, metisDatasetId);
    tuple.addParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER, europeanaIdentifier.getSourceProvidedChoAbout());
    tuple.addParameter(PluginParameterKeys.EUROPEANA_ID, europeanaIdentifier.getEuropeanaGeneratedId());
  }

  private EuropeanaGeneratedIdsMap getEuropeanaIdentifier(CommonTaskTuple commonTaskTuple, String datasetId)
      throws EuropeanaIdException {
    String document = new String(commonTaskTuple.getRecordData().getFileData(), StandardCharsets.UTF_8);
    EuropeanaIdCreator europeanIdCreator = new EuropeanaIdCreator();
    return europeanIdCreator.constructEuropeanaId(document, datasetId);
  }
}
