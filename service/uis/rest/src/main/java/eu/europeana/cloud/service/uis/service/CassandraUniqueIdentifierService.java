package eu.europeana.cloud.service.uis.service;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.dao.CassandraCloudIdDAO;
import eu.europeana.cloud.service.uis.dao.CassandraDataProviderDAO;
import eu.europeana.cloud.service.uis.dao.CassandraLocalIdDAO;
import eu.europeana.cloud.service.uis.encoder.IdGenerator;
import eu.europeana.cloud.service.uis.exception.*;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Cassandra implementation of the Unique Identifier Service
 *
 * @author Yorgos.Mamakis@ kb.nl
 */
public class CassandraUniqueIdentifierService implements UniqueIdentifierService {

    private final CassandraCloudIdDAO cloudIdDao;
    private final CassandraLocalIdDAO localIdDao;
    private final CassandraDataProviderDAO dataProviderDao;
    private final String hostList;
    private final String keyspace;
    private final String port;

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraUniqueIdentifierService.class);


    /**
     * Initialization of the service with its DAOs
     *
     * @param cloudIdDao      cloud identifier DAO
     * @param localIdDao      local identifier DAO
     * @param dataProviderDao data provider DAO
     */
    public CassandraUniqueIdentifierService(CassandraCloudIdDAO cloudIdDao, CassandraLocalIdDAO localIdDao,
                                            CassandraDataProviderDAO dataProviderDao) {
        LOGGER.info("PersistentUniqueIdentifierService starting...");
        this.cloudIdDao = cloudIdDao;
        this.localIdDao = localIdDao;
        this.dataProviderDao = dataProviderDao;
        this.hostList = cloudIdDao.getHostList();
        this.keyspace = cloudIdDao.getKeyspace();
        this.port = cloudIdDao.getPort();
        LOGGER.info("PersistentUniqueIdentifierService started successfully...");
    }

    @Override
    public CloudId createCloudId(String providerId) throws RecordExistsException, DatabaseConnectionException, ProviderDoesNotExistException, CloudIdAlreadyExistException {
        return createCloudId(providerId, IdGenerator.timeEncode(providerId));
    }


    @Override
    public CloudId createCloudId(String providerId, String recordId)
            throws DatabaseConnectionException, RecordExistsException, ProviderDoesNotExistException, CloudIdAlreadyExistException {
        LOGGER.info("createCloudId() creating cloudId providerId={}", providerId);
        if (dataProviderDao.getProvider(providerId) == null) {
            LOGGER.warn("ProviderDoesNotExistException for providerId={}", providerId);
            throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
                    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));
        }
        LOGGER.info("createCloudId() creating cloudId providerId='{}', recordId='{}'", providerId, recordId);
        if (localIdDao.searchById(providerId, recordId).isPresent()) {
            LOGGER.warn("RecordExistsException for providerId={}, recordId={}", providerId, recordId);
            throw new RecordExistsException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.RECORD_EXISTS.getHttpCode(),
                    IdentifierErrorTemplate.RECORD_EXISTS.getErrorInfo(providerId, recordId)));
        }
        String id = IdGenerator.encodeWithSha256AndBase32("/" + providerId + "/" + recordId);
        List<CloudId> cloudIds = cloudIdDao.insert(false, id, providerId, recordId);

        if(cloudIds.isEmpty()) {
            throw new CloudIdAlreadyExistException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.CLOUDID_ALREADY_EXIST.getHttpCode(),
                    IdentifierErrorTemplate.CLOUDID_ALREADY_EXIST.getErrorInfo(id)));
        }

        localIdDao.insert(providerId, recordId, id);
        CloudId cloudId = new CloudId();
        cloudId.setId(cloudIds.get(0).getId());
        LocalId lId = new LocalId();
        lId.setProviderId(providerId);
        lId.setRecordId(recordId);
        cloudId.setLocalId(lId);
        return cloudId;
    }


    @Override
    public CloudId getCloudId(String providerId, String recordId)
            throws DatabaseConnectionException, RecordDoesNotExistException {
        LOGGER.debug("getCloudId() providerId='{}', recordId='{}'", providerId, recordId);
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
            throws DatabaseConnectionException, CloudIdDoesNotExistException, IdHasBeenMappedException,
            ProviderDoesNotExistException, CloudIdAlreadyExistException {
        LOGGER.info("createIdMapping() creating mapping for cloudId='{}', providerId='{}', recordId='{}'",
                cloudId, providerId, recordId);
        if (dataProviderDao.getProvider(providerId) == null) {
            LOGGER.warn("ProviderDoesNotExistException for cloudId='{}', providerId='{}', recordId='{}'", cloudId,
                    providerId, recordId);
            throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
                    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));
        }

        List<CloudId> cloudIds = cloudIdDao.searchById(cloudId);
        if (cloudIds.isEmpty()) {
            LOGGER.warn("CloudIdDoesNotExistException for cloudId='{}', providerId='{}', recordId='{}'", cloudId,
                    providerId, recordId);
            throw new CloudIdDoesNotExistException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
                    IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo(cloudId)));
        }
        if (localIdDao.searchById(providerId, recordId).isPresent()) {
            LOGGER.warn("IdHasBeenMappedException for cloudId='{}', providerId='{}', recordId='{}'", cloudId,
                    providerId, recordId);
            throw new IdHasBeenMappedException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getHttpCode(),
                    IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getErrorInfo(providerId, recordId, cloudId)));
        }

        localIdDao.insert(providerId, recordId, cloudId);

        if (cloudIdDao.insert(false, cloudId, providerId, recordId).isEmpty()) {
            throw new CloudIdAlreadyExistException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.CLOUDID_ALREADY_EXIST.getHttpCode(),
                    IdentifierErrorTemplate.CLOUDID_ALREADY_EXIST.getErrorInfo(cloudId)));
        }

        CloudId newCloudId = new CloudId();
        newCloudId.setId(cloudId);

        LocalId lid = new LocalId();
        lid.setProviderId(providerId);
        lid.setRecordId(recordId);
        newCloudId.setLocalId(lid);
        LOGGER.info("createIdMapping() new mapping created! new cloudId='{}' for already "
                        + "existing cloudId='{}', providerId='{}', recordId='{}'", newCloudId, cloudId, providerId,
                recordId);
        return newCloudId;
    }


    @Override
    public void removeIdMapping(String providerId, String recordId)
            throws DatabaseConnectionException, ProviderDoesNotExistException {
        LOGGER.info("removeIdMapping() removing Id mapping for providerId='{}', recordId='{}' ...", providerId,
                recordId);
        if (dataProviderDao.getProvider(providerId) == null) {
            LOGGER.warn("ProviderDoesNotExistException for providerId='{}', recordId='{}'", providerId, recordId);
            throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
                    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));
        }
        localIdDao.delete(providerId, recordId);
        LOGGER.info("Id mapping removed for providerId='{}', recordId='{}'", providerId, recordId);
    }


    @Override
    public List<CloudId> deleteCloudId(String cloudId)
            throws DatabaseConnectionException, CloudIdDoesNotExistException {

        LOGGER.info("deleteCloudId() deleting cloudId='{}' ...", cloudId);
        if (cloudIdDao.searchById(cloudId).isEmpty()) {
            LOGGER.warn("CloudIdDoesNotExistException for cloudId='{}'", cloudId);
            throw new CloudIdDoesNotExistException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
                    IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo(cloudId)));
        }
        List<CloudId> localIds = cloudIdDao.searchAll(cloudId);
        for (CloudId cId : localIds) {
            localIdDao.delete(cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
            cloudIdDao.delete(cloudId, cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
        }
        LOGGER.info("CloudId deleted for cloudId='{}'", cloudId);
        return localIds;
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
            throws DatabaseConnectionException, CloudIdDoesNotExistException, IdHasBeenMappedException,
            ProviderDoesNotExistException, CloudIdAlreadyExistException {
        LOGGER.info("createIdMapping() cloudId='{}', providerId='{}'",cloudId, providerId);
        return createIdMapping(cloudId, providerId, IdGenerator.timeEncode(providerId));
    }

}
