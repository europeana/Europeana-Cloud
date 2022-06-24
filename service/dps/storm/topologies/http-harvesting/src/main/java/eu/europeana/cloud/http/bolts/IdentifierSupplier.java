package eu.europeana.cloud.http.bolts;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import org.apache.commons.lang.StringUtils;

import java.nio.charset.StandardCharsets;

public class IdentifierSupplier {

    public void prepareIdentifiers(StormTaskTuple tuple) throws EuropeanaIdException {

        String metisDatasetId = tuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
        if (StringUtils.isEmpty(metisDatasetId)) {
            throw new EuropeanaIdException("Could not create identifier - parameter " + PluginParameterKeys.METIS_DATASET_ID + " is empty!");
        }

        EuropeanaGeneratedIdsMap europeanaIdentifier = getEuropeanaIdentifier(tuple, metisDatasetId);
        tuple.addParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER, europeanaIdentifier.getSourceProvidedChoAbout());
        tuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, europeanaIdentifier.getEuropeanaGeneratedId());
    }

    private EuropeanaGeneratedIdsMap getEuropeanaIdentifier(StormTaskTuple stormTaskTuple, String datasetId) throws EuropeanaIdException {
        String document = new String(stormTaskTuple.getFileData(), StandardCharsets.UTF_8);
        EuropeanaIdCreator europeanIdCreator = new EuropeanaIdCreator();
        return europeanIdCreator.constructEuropeanaId(document, datasetId);
    }
}
