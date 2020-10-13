package eu.europeana.cloud.service.dps.storm.io;


import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Stores a Record on the cloud for the harvesting topology.
 * <p/>
 * Receives a byte array representing a Record from a tuple, creates and stores
 * a new Record on the cloud, and emits the URL of the newly created record.
 */
public class HarvestingWriteRecordBolt extends WriteRecordBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(HarvestingWriteRecordBolt.class);

    public static final String ERROR_MSG_WHILE_CREATING_CLOUD_ID = "Error while creating CloudId";
    public static final String ERROR_MSG_WHILE_MAPPING_LOCAL_CLOUD_ID = "Error while mapping localId to cloudId";
    public static final String ERROR_MSG_WHILE_GETTING_CLOUD_ID = "Error while getting CloudId";
    public static final String ERROR_MSG_RETRIES = ". Retries left: {} ";


    private String ecloudUisAddress;
    private transient UISClient uisClient;

    public HarvestingWriteRecordBolt(String ecloudMcsAddress, String ecloudUisAddress) {
        super(ecloudMcsAddress);
        this.ecloudUisAddress = ecloudUisAddress;
    }

    @Override
    public void prepare() {
        uisClient = new UISClient(ecloudUisAddress);
        super.prepare();
    }

    @Override
    protected RecordWriteParams prepareWriteParameters(StormTaskTuple stormTaskTuple) throws CloudException {
        String providerId = stormTaskTuple.getParameter(PluginParameterKeys.PROVIDER_ID);
        String localId = stormTaskTuple.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER);
        String additionalLocalIdentifier = stormTaskTuple.getParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER);
        String authenticationHeader = stormTaskTuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        String cloudId = getCloudId(authenticationHeader, providerId, localId, additionalLocalIdentifier);
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

    private String getCloudId(String authorizationHeader, String providerId, String localId, String additionalLocalIdentifier) throws CloudException {
        String result;
        CloudId cloudId;
        cloudId = getCloudId(providerId, localId, authorizationHeader);
        if (cloudId != null) {
            result = cloudId.getId();
        } else {
            result = createCloudId(providerId, localId, authorizationHeader);
        }
        if (additionalLocalIdentifier != null)
            attachAdditionalLocalIdentifier(additionalLocalIdentifier, result, providerId, authorizationHeader);
        return result;

    }

    private boolean attachAdditionalLocalIdentifier(String additionalLocalIdentifier, String cloudId, String providerId, String authorizationHeader)
            throws CloudException {
        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                return uisClient.createMapping(cloudId, providerId, additionalLocalIdentifier, AUTHORIZATION, authorizationHeader);
            } catch (Exception e) {
                if (e.getCause() instanceof IdHasBeenMappedException)
                    return true;
                if (e.getCause() instanceof ProviderDoesNotExistException) {
                    LOGGER.error(ERROR_MSG_WHILE_MAPPING_LOCAL_CLOUD_ID);
                    throw e;
                }
                if (retries-- > 0) {
                    LOGGER.warn(ERROR_MSG_WHILE_MAPPING_LOCAL_CLOUD_ID+ERROR_MSG_RETRIES, retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error(ERROR_MSG_WHILE_CREATING_CLOUD_ID);
                    throw e;
                }
            }
        }
    }

    private CloudId getCloudId(String providerId, String localId, String authenticationHeader) throws CloudException {
        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                return uisClient.getCloudId(providerId, localId, AUTHORIZATION, authenticationHeader);
            } catch (Exception e) {
                if (e.getCause() instanceof RecordDoesNotExistException)
                    return null;
                if (e.getCause() instanceof ProviderDoesNotExistException) {
                    LOGGER.error(ERROR_MSG_WHILE_GETTING_CLOUD_ID);
                    throw e;
                }
                if (retries-- > 0) {
                    LOGGER.warn(ERROR_MSG_WHILE_GETTING_CLOUD_ID+ERROR_MSG_RETRIES, retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error(ERROR_MSG_WHILE_GETTING_CLOUD_ID);
                    throw e;
                }
            }
        }
    }

    private String createCloudId(String providerId, String localId, String authenticationHeader) throws CloudException {
        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                return uisClient.createCloudId(providerId, localId, AUTHORIZATION, authenticationHeader).getId();
            } catch (Exception e) {
                if (e.getCause() instanceof ProviderDoesNotExistException) {
                    LOGGER.error(ERROR_MSG_WHILE_CREATING_CLOUD_ID);
                    throw e;
                }
                if (retries-- > 0) {
                    LOGGER.warn(ERROR_MSG_WHILE_CREATING_CLOUD_ID+ERROR_MSG_RETRIES, retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error(ERROR_MSG_WHILE_CREATING_CLOUD_ID);
                    throw e;
                }
            }
        }
    }
}



