package eu.europeana.cloud.service.uis;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.uis.dao.InMemoryCloudIdDao;
import eu.europeana.cloud.service.uis.dao.InMemoryLocalIdDao;
import eu.europeana.cloud.service.uis.encoder.Base36;
import eu.europeana.cloud.service.uis.exception.*;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * In-memory mockup of the unique identifier service
 * 
 * @see UniqueIdentifierService
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 17, 2013
 */
@Service
public class InMemoryUniqueIdentifierService implements UniqueIdentifierService {
	@Autowired
	private InMemoryCloudIdDao cloudIdDao;

	@Autowired
	private InMemoryLocalIdDao localIdDao;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(InMemoryUniqueIdentifierService.class);

	/**
	 * Creates a new instance of this class.
	 */
	public InMemoryUniqueIdentifierService() {
		// nothing to do
        LOGGER.info("InMemoryUniqueIdentifierService started successfully...");
	}

	/**
	 * Creates a new instance of this class and initializes the service with
	 * given data access objects.
	 * 
	 * @param cloudIdDao
	 *            data access for cloud identifiers
	 * @param localIdDao
	 *            data access for local identifiers
	 */
	public InMemoryUniqueIdentifierService(InMemoryCloudIdDao cloudIdDao, InMemoryLocalIdDao localIdDao) {
        LOGGER.info("InMemoryUniqueIdentifierService starting...");
		this.cloudIdDao = cloudIdDao;
		this.localIdDao = localIdDao;
        LOGGER.info("InMemoryUniqueIdentifierService started successfully.");
	}

	@Override
	public CloudId createCloudId(String... recordInfo) throws DatabaseConnectionException, RecordExistsException,
			ProviderDoesNotExistException {
        LOGGER.info("createCloudId() creating cloudId");
		String providerId = recordInfo[0];
		String recordId = recordInfo.length > 1 ? recordInfo[1] : Base36.timeEncode(providerId);
        LOGGER.info("createCloudId() creating cloudId for providerId='{}', recordId='{}'", providerId, recordId);
		String cloudId = Base36.encode(String.format("/%s/%s", providerId, recordId));
		if (!localIdDao.searchActive(providerId, recordId).isEmpty()) {
	        LOGGER.warn("RecordExistsException for providerId={}, recordId={}", providerId, recordId);
			throw new RecordExistsException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.RECORD_EXISTS.getHttpCode(),
					IdentifierErrorTemplate.RECORD_EXISTS.getErrorInfo(providerId, recordId)));
		}
		localIdDao.insert(cloudId, providerId, recordId);
		return cloudIdDao.insert(cloudId, providerId, recordId).get(0);
	}

	@Override
	public CloudId getCloudId(String providerId, String recordId) throws DatabaseConnectionException,
			RecordDoesNotExistException, ProviderDoesNotExistException {
        LOGGER.info("getCloudId() providerId='{}', recordId='{}'", providerId, recordId);
		List<CloudId> cloudIds = localIdDao.searchActive(providerId, recordId);
		if (cloudIds.isEmpty()) {
	        LOGGER.warn("RecordDoesNotExistException for providerId={}, recordId={}", providerId, recordId);
			throw new RecordDoesNotExistException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getHttpCode(),
					IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId)));
		}
		final CloudId cloudId = cloudIds.get(0);
        LOGGER.info("getCloudId() returning cloudId='{}'", cloudId);
		return cloudId;
	}

	@Override
	public List<CloudId> getLocalIdsByCloudId(String cloudId) throws DatabaseConnectionException,
			CloudIdDoesNotExistException, ProviderDoesNotExistException {
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
	public List<CloudId> getCloudIdsByProvider(String providerId, String start, int end)
			throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException {
        LOGGER.info("getCloudIdsByProvider() providerId='{}', start='{}', end='{}'", providerId, start, end);
		List<CloudId> cloudIds;

		if (start == null) {
			cloudIds = localIdDao.searchActive(providerId);
		} else {
			cloudIds = localIdDao.searchActiveWithPagination(start, end, providerId);
		}

		if (cloudIds.isEmpty()) {
	        LOGGER.warn("RecordDatasetEmptyException for providerId='{}', start='{}', end='{}'", providerId, start, end);
			throw new RecordDatasetEmptyException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.RECORDSET_EMPTY.getHttpCode(),
					IdentifierErrorTemplate.RECORDSET_EMPTY.getErrorInfo(providerId)));
		}
		return cloudIds;
	}

	@Override
	public CloudId createIdMapping(String cloudId, String providerId, String recordId) throws DatabaseConnectionException,
			CloudIdDoesNotExistException, IdHasBeenMappedException, ProviderDoesNotExistException {
        LOGGER.info("createIdMapping() creating mapping for clouId='{}', providerId='{}', recordId='{}' ...", cloudId, providerId, recordId);
		List<CloudId> localIds = localIdDao.searchActive(providerId, recordId);
		if (!localIds.isEmpty()) {
	        LOGGER.warn("IdHasBeenMappedException for clouId='{}' providerId='{}', recordId='{}'", cloudId, providerId, recordId);
			throw new IdHasBeenMappedException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getHttpCode(),
					IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getErrorInfo(providerId, recordId, cloudId)));
		}
		List<CloudId> cloudIds = cloudIdDao.searchActive(cloudId);
		if (cloudIds.isEmpty()) {
	        LOGGER.warn("CloudIdDoesNotExistException for clouId='{}' providerId='{}', recordId='{}'", cloudId, providerId, recordId);
			throw new CloudIdDoesNotExistException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
					IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo(cloudId)));
		}

		localIdDao.insert(cloudId, providerId, recordId);
		cloudIdDao.insert(cloudId, providerId, recordId);
		CloudId newCloudId = new CloudId();
		newCloudId.setId(cloudId);
		
		LocalId lid = new LocalId();
		lid.setProviderId(providerId);
		lid.setRecordId(recordId);
		newCloudId.setLocalId(lid);
        LOGGER.info("createIdMapping() new mapping created! new cloudId='{}' for already "
        		+ "existing cloudId='{}', providerId='{}', providerId='{}' ...", newCloudId, cloudId, providerId, providerId);
		return newCloudId;
	}

	@Override
	public void removeIdMapping(String providerId, String recordId) throws DatabaseConnectionException,
			ProviderDoesNotExistException, RecordIdDoesNotExistException {
        LOGGER.info("removeIdMapping() removing Id mapping for providerId='{}', recordId='{}' ...", providerId, recordId);
		localIdDao.delete(providerId, recordId);
        LOGGER.info("Id mapping removed for providerId='{}', recordId='{}'", providerId, recordId);
	}

	@Override
	public void deleteCloudId(String cloudId) throws DatabaseConnectionException, CloudIdDoesNotExistException,
			ProviderDoesNotExistException, RecordIdDoesNotExistException {
        LOGGER.info("deleteCloudId() deleting cloudId='{}' ...", cloudId);
		if (cloudIdDao.searchActive(cloudId).isEmpty()) {
	        LOGGER.warn("CloudIdDoesNotExistException for cloudId='{}'", cloudId);
			throw new CloudIdDoesNotExistException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
					IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo(cloudId)));
		}
		List<CloudId> localIds = cloudIdDao.searchActive(cloudId);
		for (CloudId cId : localIds) {
			localIdDao.delete(cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
			cloudIdDao.delete(cloudId, cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
		}
        LOGGER.info("CloudId deleted for cloudId='{}'", cloudId);
	}

	@Override
	public String getHost() {
		return "testhost";
	}

	@Override
	public String getKeyspace() {
		return "testkeyspace";
	}

	@Override
	public String getPort() {
		return "testport";
	}

	/**
	 * Empty DAOs
	 */
	public void reset() {
        LOGGER.info("reset(), reseting..");
		localIdDao.reset();
		cloudIdDao.reset();
        LOGGER.info("reset finished successfully");
	}

	@Override
	public CloudId createIdMapping(String cloudId, String providerId) throws DatabaseConnectionException,
			CloudIdDoesNotExistException, IdHasBeenMappedException, ProviderDoesNotExistException,
			RecordDatasetEmptyException {
        LOGGER.info("createIdMapping() cloudId='{}', providerId='{}'", providerId);
		return createIdMapping(cloudId, providerId, Base36.timeEncode(providerId));
	}
}
