package eu.europeana.cloud.service.dps.storm.io;


import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskTupleUtility;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import org.slf4j.LoggerFactory;

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
    protected URI createRepresentationAndUploadFile(StormTaskTuple stormTaskTuple, RecordServiceClient recordServiceClient) throws MCSException, CloudException {
        String providerId = stormTaskTuple.getParameter(PluginParameterKeys.PROVIDER_ID);
        String localId = stormTaskTuple.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER);
        String cloudId = getCloudId(stormTaskTuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER), providerId, localId);
        String representationName = stormTaskTuple.getParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME);
        if (representationName == null || representationName.isEmpty()) {
            if (stormTaskTuple.getSourceDetails() != null) {
                representationName = stormTaskTuple.getSourceDetails().getSchema();
                if (representationName == null)
                    representationName = PluginParameterKeys.PLUGIN_PARAMETERS.get(PluginParameterKeys.NEW_REPRESENTATION_NAME);
            }
        }
        return recordServiceClient.createRepresentation(cloudId, representationName, providerId, stormTaskTuple.getFileByteDataAsStream(), stormTaskTuple.getParameter(PluginParameterKeys.OUTPUT_FILE_NAME), TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.OUTPUT_MIME_TYPE));

    }

    private String getCloudId(String authorizationHeader, String providerId, String localId) throws CloudException {
        UISClient uisClient = new UISClient(ecloudUisAddress);
        uisClient.useAuthorizationHeader(authorizationHeader);
        CloudId cloudId;
        try {
            cloudId = uisClient.getCloudId(providerId, localId);
        } catch (CloudException e) {
            if (e.getCause() instanceof RecordDoesNotExistException) {
                cloudId = null;
            } else {
                throw e;
            }
        }
        if (cloudId != null) {
            return cloudId.getId();
        }
        return uisClient.createCloudId(providerId, localId).getId();
    }

}
