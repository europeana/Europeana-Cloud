package eu.europeana.cloud.service.uis.service;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.utils.LogMessageCleaner;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.dao.CassandraDataProviderDAO;
import eu.europeana.cloud.service.uis.dao.CloudIdDAO;
import eu.europeana.cloud.service.uis.dao.CloudIdLocalIdBatches;
import eu.europeana.cloud.service.uis.dao.LocalIdDAO;
import eu.europeana.cloud.service.uis.encoder.IdGenerator;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cassandra implementation of the Unique Identifier Service
 */
public class UniqueIdentifierServiceImpl implements UniqueIdentifierService {

  private static final Logger LOGGER = LoggerFactory.getLogger(UniqueIdentifierServiceImpl.class);

  private final CloudIdDAO cloudIdDao;
  private final LocalIdDAO localIdDao;
  private final CassandraDataProviderDAO dataProviderDao;
  private final CloudIdLocalIdBatches cloudIdLocalIdBatches;
  private final String hostList;
  private final String keyspace;
  private final String port;


  /**
   * Initialization of the service with its DAOs
   *
   * @param cloudIdDao cloud identifier DAO
   * @param localIdDao local identifier DAO
   * @param dataProviderDao data provider DAO
   */
  public UniqueIdentifierServiceImpl(CloudIdDAO cloudIdDao, LocalIdDAO localIdDao,
      CassandraDataProviderDAO dataProviderDao, CloudIdLocalIdBatches cloudIdLocalIdBatches) {
    LOGGER.info("PersistentUniqueIdentifierService starting...");

    this.cloudIdDao = cloudIdDao;
    this.localIdDao = localIdDao;
    this.dataProviderDao = dataProviderDao;
    this.cloudIdLocalIdBatches = cloudIdLocalIdBatches;

    this.hostList = cloudIdDao.getHostList();
    this.keyspace = cloudIdDao.getKeyspace();
    this.port = cloudIdDao.getPort();

    LOGGER.info("PersistentUniqueIdentifierService started successfully...");
  }

  @Override
  public CloudId createCloudId(String providerId) throws DatabaseConnectionException, ProviderDoesNotExistException {
    return createCloudId(providerId, IdGenerator.timeEncode(providerId));
  }


  @Override
  public CloudId createCloudId(String providerId, String recordId)
      throws DatabaseConnectionException, ProviderDoesNotExistException {
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("createCloudId() creating cloudId providerId={}",
          LogMessageCleaner.clean(providerId));
    }
    if (dataProviderDao.getProvider(providerId) == null) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn("ProviderDoesNotExistException for providerId={}",
                LogMessageCleaner.clean(providerId));
      }
      throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
          IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
          IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));
    }
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("createCloudId() creating cloudId providerId='{}', recordId='{}'",
          LogMessageCleaner.clean(providerId),
          LogMessageCleaner.clean(recordId));
    }

    var cloudIdOpt = localIdDao.searchById(providerId, recordId);
    if (cloudIdOpt.isPresent()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Record already exists providerId={}, recordId={}",
            LogMessageCleaner.clean(providerId),
            LogMessageCleaner.clean(recordId));
      }
      return cloudIdOpt.get();
    }

    String generatedCloudId = IdGenerator.encodeWithSha256AndBase32("/" + providerId + "/" + recordId);
    cloudIdLocalIdBatches.insert(providerId, recordId, generatedCloudId);

    return CloudId.builder()
                  .id(generatedCloudId)
                  .localId(LocalId.builder()
                                  .providerId(providerId)
                                  .recordId(recordId)
                                  .build())
                  .build();
  }


  @Override
  public CloudId getCloudId(String providerId, String recordId)
      throws DatabaseConnectionException, RecordDoesNotExistException {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("getCloudId() providerId='{}', recordId='{}'",
          LogMessageCleaner.clean(providerId),
          LogMessageCleaner.clean(recordId));
    }
    final CloudId cloudId = localIdDao.searchById(providerId, recordId)
                                      .orElseThrow(() -> new RecordDoesNotExistException(new IdentifierErrorInfo(
                                          IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getHttpCode(),
                                          IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId))));

    LOGGER.debug("getCloudId() returning cloudId='{}'", cloudId);
    return cloudId;
  }


  @Override
  public List<CloudId> getLocalIdsByCloudId(String cloudId)
      throws DatabaseConnectionException, CloudIdDoesNotExistException {
    LOGGER.debug("getLocalIdsByCloudId() cloudId='{}'", cloudId);
    List<CloudId> cloudIds = cloudIdDao.searchById(cloudId);
    if (cloudIds.isEmpty()) {
      LOGGER.warn("CloudIdDoesNotExistException for cloudId={}", cloudId);
      throw new CloudIdDoesNotExistException(new IdentifierErrorInfo(
          IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
          IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo(cloudId)));
    }
    LOGGER.debug("Prepared id list for cloudId={}, size={}", cloudId, cloudIds.size());
    return cloudIds;
  }

  @Override
  public CloudId createIdMapping(String cloudId, String providerId, String recordId)
      throws DatabaseConnectionException, CloudIdDoesNotExistException, ProviderDoesNotExistException {

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("createIdMapping() creating mapping for cloudId='{}', providerId='{}', recordId='{}'",
          LogMessageCleaner.clean(cloudId),
          LogMessageCleaner.clean(providerId),
          LogMessageCleaner.clean(recordId));
    }

    if (dataProviderDao.getProvider(providerId) == null) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn("ProviderDoesNotExistException for cloudId='{}', providerId='{}', recordId='{}'",
            LogMessageCleaner.clean(cloudId),
            LogMessageCleaner.clean(providerId),
            LogMessageCleaner.clean(recordId));
      }
      throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
          IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
          IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));
    }

    List<CloudId> cloudIds = cloudIdDao.searchById(cloudId);
    if (cloudIds.isEmpty()) {
      if (LOGGER.isWarnEnabled()) {
        LOGGER.warn("CloudIdDoesNotExistException for cloudId='{}', providerId='{}', recordId='{}'",
            LogMessageCleaner.clean(cloudId),
            LogMessageCleaner.clean(providerId),
            LogMessageCleaner.clean(recordId));
      }
      throw new CloudIdDoesNotExistException(new IdentifierErrorInfo(
          IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
          IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo(cloudId)));
    }

    var cloudIdOpt = localIdDao.searchById(providerId, recordId);
    if (cloudIdOpt.isPresent()) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Record already exists cloudId='{}', providerId='{}', recordId='{}'",
            LogMessageCleaner.clean(cloudId),
            LogMessageCleaner.clean(providerId),
            LogMessageCleaner.clean(recordId));
      }
      return cloudIdOpt.get();
    }

    cloudIdLocalIdBatches.insert(providerId, recordId, cloudId);

    var newCloudId = CloudId.builder()
                            .id(cloudId)
                            .localId(LocalId.builder()
                                            .providerId(providerId)
                                            .recordId(recordId)
                                            .build())
                            .build();

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info(
          "createIdMapping() new mapping created! new cloudId='{}' for already existing cloudId='{}', providerId='{}', recordId='{}'",
          newCloudId,
          LogMessageCleaner.clean(cloudId),
          LogMessageCleaner.clean(providerId),
          LogMessageCleaner.clean(recordId));
    }

    return newCloudId;
  }

  @Override
  public String getHostList() {
    return this.hostList;
  }


  @Override
  public String getKeyspace() {
    return this.keyspace;
  }


  @Override
  public String getPort() {
    return this.port;
  }


  @Override
  public CloudId createIdMapping(String cloudId, String providerId)
      throws DatabaseConnectionException, CloudIdDoesNotExistException, ProviderDoesNotExistException {

    LOGGER.info("createIdMapping() cloudId='{}', providerId='{}'", cloudId, providerId);
    return createIdMapping(cloudId, providerId, IdGenerator.timeEncode(providerId));
  }

}
