package eu.europeana.cloud.http.bolts;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import org.apache.commons.lang.StringUtils;

import java.util.UUID;

public class IdentifierSupplier {

    private static final String CLOUD_SEPARATOR = "_";

    public void prepareIdentifiers(StormTaskTuple tuple) throws EuropeanaIdException {
        final boolean useDefaultIdentifiers = useDefaultIdentifier(tuple);
        String metisDatasetId = null;
        if (!useDefaultIdentifiers) {
            metisDatasetId = tuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
            if (StringUtils.isEmpty(metisDatasetId)) {
                throw new EuropeanaIdException("Could not create identifier - parameter " + PluginParameterKeys.METIS_DATASET_ID + " is empty!");
            }
        }

        String localId;
        if (useDefaultIdentifiers) {
            localId = formulateLocalId(evaluateFileRelativePath(tuple));
        } else {
            EuropeanaGeneratedIdsMap europeanaIdentifier = getEuropeanaIdentifier(tuple, metisDatasetId);
            localId = europeanaIdentifier.getEuropeanaGeneratedId();
            tuple.addParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER, europeanaIdentifier.getSourceProvidedChoAbout());
        }
        tuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, localId);
    }

    private String evaluateFileRelativePath(StormTaskTuple tuple) {

        String fileUrl = tuple.getFileUrl();

        String taskPart = tuple.getTaskId() + "/";
        int taskPartIndex = fileUrl.indexOf(taskPart);
        if (taskPartIndex != -1) {
            return fileUrl.substring(taskPartIndex + taskPart.length());
        } else {
            return fileUrl;
        }
    }

    private boolean useDefaultIdentifier(StormTaskTuple stormTaskTuple) {
        boolean useDefaultIdentifiers = false;
        if ("true".equals(stormTaskTuple.getParameter(PluginParameterKeys.USE_DEFAULT_IDENTIFIERS))) {
            useDefaultIdentifiers = true;
        }
        return useDefaultIdentifiers;
    }

    private EuropeanaGeneratedIdsMap getEuropeanaIdentifier(StormTaskTuple stormTaskTuple, String datasetId) throws EuropeanaIdException {
        String document = new String(stormTaskTuple.getFileData());
        EuropeanaIdCreator europeanIdCreator = new EuropeanaIdCreator();
        return europeanIdCreator.constructEuropeanaId(document, datasetId);
    }

    private String formulateLocalId(String readableFilePath) {
        return new StringBuilder(readableFilePath).append(CLOUD_SEPARATOR).append(UUID.randomUUID().toString()).toString();
    }
}
