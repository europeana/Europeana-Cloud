package eu.europeana.cloud.service.dps.storm.topologies.administration;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.metis.transformation.service.EuropeanaGeneratedIdsMap;
import eu.europeana.metis.transformation.service.EuropeanaIdCreator;
import eu.europeana.metis.transformation.service.EuropeanaIdException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class DatasetAdministrationBolt extends AbstractDpsBolt {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatasetAdministrationBolt.class);

  public static final String ERROR_MSG_WHILE_CREATING_CLOUD_ID = "Error while creating CloudId";
  public static final String ERROR_MSG_WHILE_MAPPING_LOCAL_CLOUD_ID = "Error while mapping localId to cloudId";

  private final String ecloudUisAddress;
  private final String topologyUserName;
  private final String topologyUserPassword;
  private transient UISClient uisClient;

  public DatasetAdministrationBolt(CassandraProperties cassandraProperties, String ecloudUisAddress, String topologyUserName, String topologyUserPassword) {
    super(cassandraProperties);
    this.ecloudUisAddress = ecloudUisAddress;
    this.topologyUserName = topologyUserName;
    this.topologyUserPassword = topologyUserPassword;
  }

  @Override
  public void execute(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {
    LOGGER.info("Processing record: {} ...", commonTaskTuple.getRecordUri());
    try {
      String createdCloudId = prepareCloudIdentifier(commonTaskTuple);
      prepareOutputTuple(commonTaskTuple, createdCloudId);

      LOGGER.info("Emitting {}", commonTaskTuple);
      outputCollector.emit(anchorTuple, commonTaskTuple.toStormTuple());
      outputCollector.ack(anchorTuple);
    } catch (RetryInterruptedException e) {
      handleInterruption(e, anchorTuple);
    } catch (Exception e) {
      LOGGER.error("DatasetAdministrationBolt error:{}", e.getMessage());
      emitErrorNotification(anchorTuple, commonTaskTuple, e.getMessage(), ExceptionUtils.getStackTrace(e));
      outputCollector.ack(anchorTuple);
    }
  }

  private String prepareCloudIdentifier(CommonTaskTuple commonTaskTuple) throws EuropeanaIdException, CloudException {
    RecordLocalIdentifiers localIdentifiers = generateLocalIdentifiers(commonTaskTuple);
    LOGGER.info("Generated local identifiers: {}", localIdentifiers);
    String createdCloudId = createCloudId(commonTaskTuple.getParameter(PluginParameterKeys.PROVIDER_ID), localIdentifiers.getPrimary());

    for (String identifier : localIdentifiers.getSecondary()) {
      addAdditionalIdentifiers(createdCloudId, commonTaskTuple.getParameter(PluginParameterKeys.PROVIDER_ID), identifier);
    }
    return createdCloudId;
  }

  private RecordLocalIdentifiers generateLocalIdentifiers(CommonTaskTuple tuple) throws EuropeanaIdException {
    String metisDatasetId = tuple.getParameter(PluginParameterKeys.METIS_DATASET_ID);
    if (StringUtils.isEmpty(metisDatasetId)) {
      throw new EuropeanaIdException("Could not create identifier - parameter " + PluginParameterKeys.METIS_DATASET_ID + " is empty!");
    }

    EuropeanaIdCreator europeanIdCreator = new EuropeanaIdCreator();
    EuropeanaGeneratedIdsMap europeanaIdentifier =
            europeanIdCreator.constructEuropeanaId(
                    new String(tuple.getRecordData().getFileData(), StandardCharsets.UTF_8), metisDatasetId);

    return RecordLocalIdentifiers.builder()
            .primary(europeanaIdentifier.getEuropeanaGeneratedId())
            .secondary(List.of(europeanaIdentifier.getSourceProvidedChoAbout(), tuple.getCloudId())).build();
  }

  private String createCloudId(String providerId, String europeanaId) throws CloudException {
    return RetryableMethodExecutor.executeOnRest(ERROR_MSG_WHILE_CREATING_CLOUD_ID, () ->
            uisClient.createCloudId(providerId, europeanaId).getId());
  }

  private void addAdditionalIdentifiers(String cloudId, String providerId, String additionalLocalIdentifier) throws CloudException {
    RetryableMethodExecutor.executeOnRest(ERROR_MSG_WHILE_MAPPING_LOCAL_CLOUD_ID, () ->
            uisClient.createMapping(cloudId, providerId, additionalLocalIdentifier)
    );
  }

  private void prepareOutputTuple(CommonTaskTuple commonTaskTuple, String createdCloudId) {
    commonTaskTuple.setFileData(commonTaskTuple.getFileData());
    commonTaskTuple
            .addParameter(PluginParameterKeys.CLOUD_ID, createdCloudId);
  }

  @Override
  public void prepare() {
    uisClient = new UISClient(ecloudUisAddress, topologyUserName, topologyUserPassword);
  }
}
