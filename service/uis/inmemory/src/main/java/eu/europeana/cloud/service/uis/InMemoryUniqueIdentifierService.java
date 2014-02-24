package eu.europeana.cloud.service.uis;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.uis.dao.InMemoryCloudIdDao;
import eu.europeana.cloud.service.uis.dao.InMemoryLocalIdDao;
import eu.europeana.cloud.service.uis.encoder.Base36;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.RecordExistsException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;

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

	/**
	 * Creates a new instance of this class.
	 */
	public InMemoryUniqueIdentifierService() {
		// nothing to do
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
		this.cloudIdDao = cloudIdDao;
		this.localIdDao = localIdDao;
	}

	@Override
	public CloudId createCloudId(String... recordInfo) throws DatabaseConnectionException, RecordExistsException,
			ProviderDoesNotExistException {
		String providerId = recordInfo[0];
		String recordId = recordInfo.length > 1 ? recordInfo[1] : Base36.timeEncode(providerId);
		String cloudId = Base36.encode(String.format("/%s/%s", providerId, recordId));
		if (!localIdDao.searchActive(providerId, recordId).isEmpty()) {
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
		List<CloudId> cloudIds = localIdDao.searchActive(providerId, recordId);
		if (cloudIds.isEmpty()) {
			throw new RecordDoesNotExistException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getHttpCode(),
					IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST.getErrorInfo(providerId, recordId)));
		}
		return cloudIds.get(0);
	}

	@Override
	public List<CloudId> getLocalIdsByCloudId(String cloudId) throws DatabaseConnectionException,
			CloudIdDoesNotExistException, ProviderDoesNotExistException {
		List<CloudId> cloudIds = cloudIdDao.searchActive(cloudId);
		if (cloudIds.isEmpty()) {
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
		List<CloudId> cloudIds;

		if (start == null) {
			cloudIds = localIdDao.searchActive(providerId);
		} else {
			cloudIds = localIdDao.searchActiveWithPagination(start, end, providerId);
		}

		if (cloudIds.isEmpty()) {
			throw new RecordDatasetEmptyException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.RECORDSET_EMPTY.getHttpCode(),
					IdentifierErrorTemplate.RECORDSET_EMPTY.getErrorInfo(providerId)));
		}
		return cloudIds;
	}

	@Override
	public CloudId createIdMapping(String cloudId, String providerId, String recordId) throws DatabaseConnectionException,
			CloudIdDoesNotExistException, IdHasBeenMappedException, ProviderDoesNotExistException {
		List<CloudId> localIds = localIdDao.searchActive(providerId, recordId);
		if (!localIds.isEmpty()) {
			throw new IdHasBeenMappedException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getHttpCode(),
					IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getErrorInfo(providerId, recordId, cloudId)));
		}
		List<CloudId> cloudIds = cloudIdDao.searchActive(cloudId);
		if (cloudIds.isEmpty()) {
			throw new CloudIdDoesNotExistException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
					IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo(cloudId)));
		}

		localIdDao.insert(cloudId, providerId, recordId);
		cloudIdDao.insert(cloudId, providerId, recordId);
		CloudId clId = new CloudId();
		clId.setId(cloudId);
		
		LocalId lid = new LocalId();
		lid.setProviderId(providerId);
		lid.setRecordId(recordId);
		clId.setLocalId(lid);
		return clId;
	}

	@Override
	public void removeIdMapping(String providerId, String recordId) throws DatabaseConnectionException,
			ProviderDoesNotExistException, RecordIdDoesNotExistException {

		localIdDao.delete(providerId, recordId);

	}

	@Override
	public void deleteCloudId(String cloudId) throws DatabaseConnectionException, CloudIdDoesNotExistException,
			ProviderDoesNotExistException, RecordIdDoesNotExistException {
		if (cloudIdDao.searchActive(cloudId).isEmpty()) {
			throw new CloudIdDoesNotExistException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getHttpCode(),
					IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST.getErrorInfo(cloudId)));
		}
		List<CloudId> localIds = cloudIdDao.searchActive(cloudId);
		for (CloudId cId : localIds) {
			localIdDao.delete(cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
			cloudIdDao.delete(cloudId, cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
		}
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
	 * Empty daos
	 */
	public void reset() {
		localIdDao.reset();
		cloudIdDao.reset();
	}

	@Override
	public CloudId createIdMapping(String cloudId, String providerId) throws DatabaseConnectionException,
			CloudIdDoesNotExistException, IdHasBeenMappedException, ProviderDoesNotExistException,
			RecordDatasetEmptyException {
		return createIdMapping(cloudId, providerId, Base36.timeEncode(providerId));
	}
}
