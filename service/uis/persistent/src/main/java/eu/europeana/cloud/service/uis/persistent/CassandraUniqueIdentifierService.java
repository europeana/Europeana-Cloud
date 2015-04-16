package eu.europeana.cloud.service.uis.persistent;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.persistent.dao.CassandraDataProviderDAO;
import eu.europeana.cloud.service.uis.persistent.dao.CassandraCloudIdDAO;
import eu.europeana.cloud.service.uis.persistent.dao.CassandraLocalIdDAO;
import eu.europeana.cloud.service.uis.encoder.IdGenerator;
import eu.europeana.cloud.service.uis.exception.CloudIdAlreadyExistException;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordExistsException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;

/**
 * Cassandra implementation of the Unique Identifier Service
 * 
 * @author Yorgos.Mamakis@ kb.nl
 */
@Service
public class CassandraUniqueIdentifierService implements
	UniqueIdentifierService {

    private CassandraCloudIdDAO cloudIdDao;
    private CassandraLocalIdDAO localIdDao;
    private CassandraDataProviderDAO dataProviderDao;
    private String hostList;
    private String keyspace;
    private String port;

    private static final Logger LOGGER = LoggerFactory
	    .getLogger(CassandraUniqueIdentifierService.class);

    /**
     * Initialization of the service with its DAOs
     * 
     * @param cloudIdDao
     *            cloud identifier DAO
     * @param localIdDao
     *            local identifier DAO
     * @param dataProviderDao
     */
    public CassandraUniqueIdentifierService(CassandraCloudIdDAO cloudIdDao,
	    CassandraLocalIdDAO localIdDao,
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
    public CloudId createCloudId(String... recordInfo)
	    throws DatabaseConnectionException, RecordExistsException,
	    ProviderDoesNotExistException, RecordDatasetEmptyException,
	    CloudIdDoesNotExistException, CloudIdAlreadyExistException {
	LOGGER.info("createCloudId() creating cloudId");
	String providerId = recordInfo[0];
	LOGGER.info("createCloudId() creating cloudId providerId={}",
		providerId);
	if (dataProviderDao.getProvider(providerId) == null) {
	    LOGGER.warn("ProviderDoesNotExistException for providerId={}",
		    providerId);
	    throw new ProviderDoesNotExistException(
		    new IdentifierErrorInfo(
			    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
				    .getHttpCode(),
			    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
				    .getErrorInfo(providerId)));
	}
	String recordId = recordInfo.length > 1 ? recordInfo[1] : IdGenerator
		.timeEncode(providerId);
	LOGGER.info(
		"createCloudId() creating cloudId providerId='{}', recordId='{}'",
		providerId, recordId);
	if (!localIdDao.searchActive(providerId, recordId).isEmpty()) {
	    LOGGER.warn("RecordExistsException for providerId={}, recordId={}",
		    providerId, recordId);
	    throw new RecordExistsException(new IdentifierErrorInfo(
		    IdentifierErrorTemplate.RECORD_EXISTS.getHttpCode(),
		    IdentifierErrorTemplate.RECORD_EXISTS.getErrorInfo(
			    providerId, recordId)));
	}
	String id = IdGenerator.encodeWithSha256AndBase32("/" + providerId + "/" + recordId);
	List<CloudId> cloudIds = cloudIdDao.insert(false, id, providerId,
		recordId);
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
	    throws DatabaseConnectionException, RecordDoesNotExistException,
	    ProviderDoesNotExistException, RecordDatasetEmptyException {
	LOGGER.info("getCloudId() providerId='{}', recordId='{}'", providerId,
		recordId);
	List<CloudId> cloudIds = localIdDao.searchActive(providerId, recordId);
	if (cloudIds.isEmpty()) {
	    throw new RecordDoesNotExistException(
		    new IdentifierErrorInfo(
			    IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST
				    .getHttpCode(),
			    IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST
				    .getErrorInfo(providerId, recordId)));
	}
	final CloudId cloudId = cloudIds.get(0);
	LOGGER.info("getCloudId() returning cloudId='{}'", cloudId);
	return cloudId;
    }

    @Override
    public List<CloudId> getLocalIdsByCloudId(String cloudId)
            throws DatabaseConnectionException, CloudIdDoesNotExistException, ProviderDoesNotExistException,
            RecordDatasetEmptyException {
        LOGGER.info("getLocalIdsByCloudId() cloudId='{}'", cloudId);
        List<CloudId> cloudIds = cloudIdDao.searchActive(cloudId);
        if (cloudIds.isEmpty()) {
            LOGGER.warn("CloudIdDoesNotExistException for cloudId={}", cloudId);
            throw new CloudIdDoesNotExistException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
                    IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo(cloudId)));
        }
        List<CloudId> localIds = new ArrayList<>();
        for (CloudId cId : cloudIds) {
            if (localIdDao.searchActive(cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId()).size() > 0) {
                localIds.add(cId);
            }
        }
        return localIds;

    }

    @Override
    public List<CloudId> getLocalIdsByProvider(String providerId, String start, int end)
            throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException {

        LOGGER.info("getLocalIdsByProvider() providerId='{}', start='{}', end='{}'", providerId, end);
        if (dataProviderDao.getProvider(providerId) == null) {
            LOGGER.warn("ProviderDoesNotExistException for providerId='{}', start='{}', end='{}'", providerId, start,
                end);
            throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
                    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));
        }
        List<CloudId> cloudIds = null;
        if (start == null) {
            cloudIds = localIdDao.searchActive(providerId);
        } else {
            cloudIds = localIdDao.searchActiveWithPagination(start, end, providerId);
        }
        List<CloudId> localIds = new ArrayList<>();
        for (CloudId cloudId : cloudIds) {
            localIds.add(cloudId);
        }
        return localIds;
    }

    @Override
    public List<CloudId> getCloudIdsByProvider(String providerId, String start,
	    int end) throws DatabaseConnectionException,
	    ProviderDoesNotExistException, RecordDatasetEmptyException {

	LOGGER.info(
		"getCloudIdsByProvider() providerId='{}', start='{}', end='{}'",
		providerId, start, end);
	if (dataProviderDao.getProvider(providerId) == null) {
	    LOGGER.warn(
		    "ProviderDoesNotExistException for providerId='{}', start='{}', end='{}'",
		    providerId, start, end);
	    throw new ProviderDoesNotExistException(
		    new IdentifierErrorInfo(
			    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
				    .getHttpCode(),
			    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
				    .getErrorInfo(providerId)));
	}
	if (start == null) {
	    return localIdDao.searchActive(providerId);
	} else {
	    return localIdDao
		    .searchActiveWithPagination(start, end, providerId);
	}
    }

    @Override
    public CloudId createIdMapping(String cloudId, String providerId,
	    String recordId) throws DatabaseConnectionException,
	    CloudIdDoesNotExistException, IdHasBeenMappedException,
	    ProviderDoesNotExistException, RecordDatasetEmptyException,
	    CloudIdAlreadyExistException {
	LOGGER.info(
		"createIdMapping() creating mapping for cloudId='{}', providerId='{}', providerId='{}' ...",
		cloudId, providerId, providerId);
	if (dataProviderDao.getProvider(providerId) == null) {
	    LOGGER.warn(
		    "ProviderDoesNotExistException for cloudId='{}', providerId='{}', recordId='{}'",
		    cloudId, providerId, recordId);
	    throw new ProviderDoesNotExistException(
		    new IdentifierErrorInfo(
			    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
				    .getHttpCode(),
			    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
				    .getErrorInfo(providerId)));
	}

	List<CloudId> cloudIds = cloudIdDao.searchActive(cloudId);
	if (cloudIds.isEmpty()) {
	    LOGGER.warn(
		    "CloudIdDoesNotExistException for cloudId='{}', providerId='{}', recordId='{}'",
		    cloudId, providerId, recordId);
	    throw new CloudIdDoesNotExistException(
		    new IdentifierErrorInfo(
			    IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST
				    .getHttpCode(),
			    IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST
				    .getErrorInfo(cloudId)));
	}
	List<CloudId> localIds = localIdDao.searchActive(providerId, recordId);
	if (!localIds.isEmpty()) {
	    LOGGER.warn(
		    "IdHasBeenMappedException for cloudId='{}', providerId='{}', recordId='{}'",
		    cloudId, providerId, recordId);
	    throw new IdHasBeenMappedException(new IdentifierErrorInfo(
		    IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getHttpCode(),
		    IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getErrorInfo(
			    providerId, recordId, cloudId)));
	}

	localIdDao.insert(providerId, recordId, cloudId);

	cloudIdDao.insert(false, cloudId, providerId, recordId);

	CloudId newCloudId = new CloudId();
	newCloudId.setId(cloudId);

	LocalId lid = new LocalId();
	lid.setProviderId(providerId);
	lid.setRecordId(recordId);
	newCloudId.setLocalId(lid);
	LOGGER.info(
		"createIdMapping() new mapping created! new cloudId='{}' for already "
			+ "existing cloudId='{}', providerId='{}', providerId='{}' ...",
		newCloudId, cloudId, providerId, providerId);
	return newCloudId;
    }

    @Override
    public void removeIdMapping(String providerId, String recordId)
	    throws DatabaseConnectionException, ProviderDoesNotExistException,
	    RecordIdDoesNotExistException {
	LOGGER.info(
		"removeIdMapping() removing Id mapping for providerId='{}', recordId='{}' ...",
		providerId, recordId);
	if (dataProviderDao.getProvider(providerId) == null) {
	    LOGGER.warn(
		    "ProviderDoesNotExistException for providerId='{}', recordId='{}'",
		    providerId, recordId);
	    throw new ProviderDoesNotExistException(
		    new IdentifierErrorInfo(
			    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
				    .getHttpCode(),
			    IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
				    .getErrorInfo(providerId)));
	}
	localIdDao.delete(providerId, recordId);
	LOGGER.info("Id mapping removed for providerId='{}', recordId='{}'",
		providerId, recordId);
    }

    @Override
    public void deleteCloudId(String cloudId)
	    throws DatabaseConnectionException, CloudIdDoesNotExistException {

	LOGGER.info("deleteCloudId() deleting cloudId='{}' ...", cloudId);
	if (cloudIdDao.searchActive(cloudId).isEmpty()) {
	    LOGGER.warn("CloudIdDoesNotExistException for cloudId='{}'",
		    cloudId);
	    throw new CloudIdDoesNotExistException(
		    new IdentifierErrorInfo(
			    IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST
				    .getHttpCode(),
			    IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST
				    .getErrorInfo(cloudId)));
	}
	List<CloudId> localIds = cloudIdDao.searchAll(cloudId);
	for (CloudId cId : localIds) {
	    localIdDao.delete(cId.getLocalId().getProviderId(), cId
		    .getLocalId().getRecordId());
	    cloudIdDao.delete(cloudId, cId.getLocalId().getProviderId(), cId
		    .getLocalId().getRecordId());
	}
	LOGGER.info("CloudId deleted for cloudId='{}'", cloudId);
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
	    throws DatabaseConnectionException, CloudIdDoesNotExistException,
	    IdHasBeenMappedException, ProviderDoesNotExistException,
	    RecordDatasetEmptyException, CloudIdAlreadyExistException {
	LOGGER.info("createIdMapping() cloudId='{}', providerId='{}'",
		providerId);
	return createIdMapping(cloudId, providerId,
		IdGenerator.timeEncode(providerId));
    }

}
