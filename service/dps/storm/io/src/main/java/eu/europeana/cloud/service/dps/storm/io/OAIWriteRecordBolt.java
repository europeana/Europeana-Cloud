package eu.europeana.cloud.service.dps.storm.io;


import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskTupleUtility;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.LoggerFactory;

import java.net.URI;


/**
 * Stores a Record on the cloud for OAI-PMH topology.
 * <p/>
 * Receives a byte array representing a Record from a tuple, creates and stores
 * a new Record on the cloud, and emits the URL of the newly created record.
 */
public class OAIWriteRecordBolt extends WriteRecordBolt {
    private String ecloudUisAddress;

    public OAIWriteRecordBolt(String ecloudMcsAddress, String ecloudUisAddress) {
        super(ecloudMcsAddress);
        this.ecloudUisAddress = ecloudUisAddress;
        LOGGER = LoggerFactory.getLogger(OAIWriteRecordBolt.class);
    }


    @Override
    protected URI createRepresentationAndUploadFile(StormTaskTuple stormTaskTuple, RecordServiceClient recordServiceClient) throws MCSException, CloudException {
        String providerId = stormTaskTuple.getParameter(PluginParameterKeys.PROVIDER_ID);
        String localId = stormTaskTuple.getParameter(PluginParameterKeys.OAI_IDENTIFIER);
        String cloudId = getCloudId(stormTaskTuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER), providerId, localId);
        return recordServiceClient.createRepresentation(cloudId, stormTaskTuple.getSourceDetails().getSchema(), providerId, stormTaskTuple.getFileByteDataAsStream(), stormTaskTuple.getParameter(PluginParameterKeys.OUTPUT_FILE_NAME), TaskTupleUtility.getParameterFromTuple(stormTaskTuple, PluginParameterKeys.OUTPUT_MIME_TYPE));

    }

    public String getCloudId(String authorizationHeader, String providerId, String localId) throws
            CloudException {
        UISClient uisClient = new UISClient(ecloudUisAddress);
        uisClient.useAuthorizationHeader(authorizationHeader);
        CloudId cloudId = uisClient.getCloudId(providerId, localId);
        if (cloudId != null)
            return cloudId.getId();
        return uisClient.createCloudId(providerId, localId).getId();
    }

}
