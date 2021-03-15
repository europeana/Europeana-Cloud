package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Stores a Record on the cloud for the harvesting topology.
 * <p/>
 * Receives a byte array representing a Record from a tuple, creates and stores
 * a new Record on the cloud, and emits the URL of the newly created record.
 */
public class HarvestingWriteRecordBolt extends WriteRecordBolt {
    public static final String ERROR_MSG_WHILE_CREATING_CLOUD_ID = "Error while creating CloudId";
    public static final String ERROR_MSG_WHILE_MAPPING_LOCAL_CLOUD_ID = "Error while mapping localId to cloudId";
    public static final String ERROR_MSG_WHILE_GETTING_CLOUD_ID = "Error while getting CloudId";
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(HarvestingWriteRecordBolt.class);
    private final String ecloudUisAddress;
    private transient UISClient uisClient;

    public HarvestingWriteRecordBolt(String ecloudMcsAddress, String ecloudUisAddress) {
        super(ecloudMcsAddress);
        this.ecloudUisAddress = ecloudUisAddress;
    }

    @Override
    public void prepare() {
        LOGGER.info("Preparing bolt for UIS address: {}", ecloudUisAddress);
        uisClient = new UISClient(ecloudUisAddress);
        super.prepare();
    }

    @Override
    protected boolean ignoreDeleted() {
        return false;
    }

    @Override
    protected RecordWriteParams prepareWriteParameters(StormTaskTuple stormTaskTuple) throws CloudException {
        String providerId = stormTaskTuple.getParameter(PluginParameterKeys.PROVIDER_ID);
        String authenticationHeader = stormTaskTuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        String cloudId = getCloudId(authenticationHeader, providerId, stormTaskTuple.getIdentifiersToUse());
        String representationName = stormTaskTuple.getParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME);
        if ((representationName == null || representationName.isEmpty()) && stormTaskTuple.getSourceDetails() != null) {
            representationName = stormTaskTuple.getParameter(PluginParameterKeys.SCHEMA_NAME);
            if (representationName == null)
                representationName = PluginParameterKeys.PLUGIN_PARAMETERS.get(PluginParameterKeys.NEW_REPRESENTATION_NAME);
        }
        RecordWriteParams writeParams = new RecordWriteParams();
        writeParams.setCloudId(cloudId);
        writeParams.setRepresentationName(representationName);
        writeParams.setProviderId(providerId);
        return writeParams;
    }

    private String getCloudId(String authorizationHeader, String providerId, List<String> identifiersToUse) throws CloudException {
        if (identifiersToUse.isEmpty())
            throw new CloudException("List of local identifiers has to be longer than 1", new RecordIdDoesNotExistException(new ErrorInfo()));
        String result;
        CloudId cloudId;
        cloudId = getCloudId(providerId, identifiersToUse.get(0), authorizationHeader);
        if (cloudId != null) {
            result = cloudId.getId();
        } else {
            result = createCloudId(providerId, identifiersToUse.get(0), authorizationHeader);
        }
        for (int i = 1; i < identifiersToUse.size(); i++) {
            attachAdditionalLocalIdentifier(identifiersToUse.get(i), result, providerId, authorizationHeader);
        }
        return result;
    }

    private CloudId getCloudId(String providerId, String localId, String authenticationHeader) throws CloudException {
        return RetryableMethodExecutor.executeOnRest(ERROR_MSG_WHILE_GETTING_CLOUD_ID, () -> {
            try {
                return uisClient.getCloudId(providerId, localId, AUTHORIZATION, authenticationHeader);
            } catch (Exception e) {
                if (e.getCause() instanceof RecordDoesNotExistException) {
                    return null;
                }
                throw e;
            }
        });
    }

    private String createCloudId(String providerId, String localId, String authenticationHeader) throws CloudException {
        return RetryableMethodExecutor.executeOnRest(ERROR_MSG_WHILE_CREATING_CLOUD_ID, () ->
                uisClient.createCloudId(providerId, localId, AUTHORIZATION, authenticationHeader).getId());
    }

    private boolean attachAdditionalLocalIdentifier(String additionalLocalIdentifier, String cloudId, String providerId, String authorizationHeader)
            throws CloudException {
        return RetryableMethodExecutor.executeOnRest(ERROR_MSG_WHILE_MAPPING_LOCAL_CLOUD_ID, () -> {
            try {
                return uisClient.createMapping(cloudId, providerId, additionalLocalIdentifier, AUTHORIZATION, authorizationHeader);
            } catch (Exception e) {
                if (e.getCause() instanceof IdHasBeenMappedException)
                    return true;
                throw e;
            }
        });
    }
}



