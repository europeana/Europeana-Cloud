package eu.europeana.cloud.service.dps.storm.io;


import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import java.net.MalformedURLException;

/**
 * Stores a Record on the cloud for the harvesting topology.
 * <p/>
 * Receives a byte array representing a Record from a tuple, creates and stores a new Record on the cloud, and emits the URL of
 * the newly created record.
 */
public class HarvestingWriteRecordBolt extends WriteRecordBolt {

  public static final String ERROR_MSG_WHILE_CREATING_CLOUD_ID = "Error while creating CloudId";
  public static final String ERROR_MSG_WHILE_MAPPING_LOCAL_CLOUD_ID = "Error while mapping localId to cloudId";
  private static final long serialVersionUID = 1L;
  private final String ecloudUisAddress;
  private final String topologyUserName;
  private final String topologyUserPassword;
  private transient UISClient uisClient;

  public HarvestingWriteRecordBolt(CassandraProperties cassandraProperties,
      String ecloudMcsAddress,
      String ecloudUisAddress,
      String topologyUserName,
      String topologyUserPassword) {
    super(cassandraProperties, ecloudMcsAddress, topologyUserName, topologyUserPassword, "oai_topology");
    this.topologyUserName = topologyUserName;
    this.topologyUserPassword = topologyUserPassword;
    this.ecloudUisAddress = ecloudUisAddress;
  }


  @Override
  public void prepare() {
    uisClient = new UISClient(ecloudUisAddress, topologyUserName, topologyUserPassword);
    super.prepare();
  }

  private String getCloudId(String providerId, String localId, String additionalLocalIdentifier) throws CloudException {
    String result = createCloudId(providerId, localId);

    if (additionalLocalIdentifier != null) {
      attachAdditionalLocalIdentifier(additionalLocalIdentifier, result, providerId);
    }

    return result;

  }

  private void attachAdditionalLocalIdentifier(String additionalLocalIdentifier, String cloudId, String providerId)
      throws CloudException {
    RetryableMethodExecutor.executeOnRest(ERROR_MSG_WHILE_MAPPING_LOCAL_CLOUD_ID, () ->
        uisClient.createMapping(cloudId, providerId, additionalLocalIdentifier)
    );
  }

  private String createCloudId(String providerId, String localId) throws CloudException {
    return RetryableMethodExecutor.executeOnRest(ERROR_MSG_WHILE_CREATING_CLOUD_ID, () ->
        uisClient.createCloudId(providerId, localId).getId());
  }

  @Override
  protected RecordWriteParams prepareWriteParameters(StormTaskTuple stormTaskTuple) throws CloudException, MalformedURLException {
    String providerId = stormTaskTuple.getParameter(PluginParameterKeys.PROVIDER_ID);
    String localId = stormTaskTuple.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER);
    String additionalLocalIdentifier = stormTaskTuple.getParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER);
    String cloudId = getCloudId(providerId, localId, additionalLocalIdentifier);
    String representationName = stormTaskTuple.getParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME);
    if ((representationName == null || representationName.isEmpty()) && stormTaskTuple.getSourceDetails() != null) {
      representationName = stormTaskTuple.getParameter(PluginParameterKeys.SCHEMA_NAME);
      if (representationName == null) {
        representationName = PluginParameterKeys.PLUGIN_PARAMETERS.get(PluginParameterKeys.NEW_REPRESENTATION_NAME);
      }
    }
    var writeParams = new RecordWriteParams();
    writeParams.setCloudId(cloudId);
    writeParams.setRepresentationName(representationName);
    writeParams.setProviderId(providerId);
    writeParams.setNewVersion(generateNewVersionId(stormTaskTuple));
    writeParams.setNewFileName(generateNewFileName(stormTaskTuple));
    writeParams.setDataSetId(StormTaskTupleHelper.extractDatasetId(stormTaskTuple));
    return writeParams;
  }
}



