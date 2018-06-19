package eu.europeana.cloud.service.dps.storm.io;


import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskTupleUtility;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;


/**
 * Stores a Record on the cloud for the harvesting topology.
 * <p/>
 * Receives a byte array representing a Record from a tuple, creates and stores
 * a new Record on the cloud, and emits the URL of the newly created record.
 */
public class HarvestingWriteRecordBolt extends WriteRecordBolt {
    private String ecloudUisAddress;

    public HarvestingWriteRecordBolt(String ecloudMcsAddress, String ecloudUisAddress) {
        super(ecloudMcsAddress);
        this.ecloudUisAddress = ecloudUisAddress;
        LOGGER = LoggerFactory.getLogger(HarvestingWriteRecordBolt.class);
    }


    @Override
    protected URI createRepresentationAndUploadFile(StormTaskTuple stormTaskTuple, RecordServiceClient recordServiceClient) throws IOException, MCSException, CloudException, DriverException {
        String providerId = stormTaskTuple.getParameter(PluginParameterKeys.PROVIDER_ID);
        String localId = stormTaskTuple.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER);
        String additionalLocalIdentifier = stormTaskTuple.getParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER);
        String cloudId = getCloudId(stormTaskTuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER), providerId, localId, additionalLocalIdentifier);
        String representationName = stormTaskTuple.getParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME);
        if (representationName == null || representationName.isEmpty()) {
            if (stormTaskTuple.getSourceDetails() != null) {
                representationName = stormTaskTuple.getParameter(PluginParameterKeys.SCHEMA_NAME);
                if (representationName == null)
                    representationName = PluginParameterKeys.PLUGIN_PARAMETERS.get(PluginParameterKeys.NEW_REPRESENTATION_NAME);
            }
        }
        return createRepresentation(stormTaskTuple, recordServiceClient, providerId, cloudId, representationName);

    }

    private URI createRepresentation(StormTaskTuple stormTaskTuple, RecordServiceClient recordServiceClient, String providerId, String cloudId, String representationName) throws IOException, MCSException, DriverException {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                return recordServiceClient.createRepresentation(cloudId, representationName, providerId, stormTaskTuple.getFileByteDataAsStream(), stormTaskTuple.getParameter(PluginParameterKeys.OUTPUT_FILE_NAME), TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.OUTPUT_MIME_TYPE));
            } catch (MCSException | DriverException e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while creating Representation. Retries left:{} ", retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while creating Representation.");
                    throw e;
                }
            }
        }
    }


    private String getCloudId(String authorizationHeader, String providerId, String localId, String additionalLocalIdentifier) throws CloudException {
        String result;
        UISClient uisClient = new UISClient(ecloudUisAddress);
        uisClient.useAuthorizationHeader(authorizationHeader);
        CloudId cloudId;
        cloudId = getCloudId(providerId, localId, uisClient);
        if (cloudId != null) {
            result = cloudId.getId();
        } else {
            result = createCloudId(providerId, localId, uisClient);
        }
        if (additionalLocalIdentifier != null)
            attachAdditionalLocalIdentifier(additionalLocalIdentifier, result, providerId, uisClient);
        return result;
    }

    private boolean attachAdditionalLocalIdentifier(String additionalLocalIdentifier, String cloudId, String providerId, UISClient uisClient)
            throws CloudException {
        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                return uisClient.createMapping(cloudId, providerId, additionalLocalIdentifier);
            } catch (CloudException e) {
                if (e.getCause() instanceof IdHasBeenMappedException)
                    return true;
                if (retries-- > 0) {
                    LOGGER.warn("Error while mapping localId to cloudId. Retries left: " + retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while creating CloudId.");
                    throw e;
                }
            }
        }
    }

    private CloudId getCloudId(String providerId, String localId, UISClient uisClient) throws CloudException {
        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                return uisClient.getCloudId(providerId, localId);
            } catch (CloudException e) {
                if (e.getCause() instanceof RecordDoesNotExistException)
                    return null;
                if (retries-- > 0) {
                    LOGGER.warn("Error while getting CloudId. Retries left: " + retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while getting CloudId.");
                    throw e;
                }
            }
        }
    }

    private String createCloudId(String providerId, String localId, UISClient uisClient) throws CloudException {
        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                return uisClient.createCloudId(providerId, localId).getId();
            } catch (CloudException e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while creating CloudId. Retries left: " + retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while creating CloudId.");
                    throw e;
                }
            }
        }
    }
}



