package eu.europeana.cloud.service.uis;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.exceptions.DatabaseConnectionException;
import eu.europeana.cloud.exceptions.GlobalIdDoesNotExistException;
import eu.europeana.cloud.exceptions.IdHasBeenMappedException;
import eu.europeana.cloud.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordDatasetEmptyException;
import eu.europeana.cloud.exceptions.RecordDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordExistsException;
import eu.europeana.cloud.exceptions.RecordIdDoesNotExistException;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.database.dao.CloudIdDao;
import eu.europeana.cloud.service.uis.database.dao.LocalIdDao;
import eu.europeana.cloud.service.uis.encoder.Base36;

/**
 * Cassandra implementation of the Unique Identifier Service
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
@Service
public class PersistentUniqueIdentifierService implements UniqueIdentifierService {

	private CloudIdDao cloudIdDao;
	private LocalIdDao localIdDao;

	private String host;
	private String keyspace;
	private String port;

	/**
	 * Initialization of the service with its DAOs
	 * @param cloudIdDao The cloud identifier Dao
	 * @param localIdDao The local identifieir Dao
	 */
	public PersistentUniqueIdentifierService(CloudIdDao cloudIdDao, LocalIdDao localIdDao) {
		this.cloudIdDao = cloudIdDao;
		this.localIdDao = localIdDao;
		this.host = cloudIdDao.getHost();
		this.keyspace = cloudIdDao.getKeyspace();
		this.port = cloudIdDao.getPort();
	}

	@Override
	public CloudId createGlobalId(String providerId, String recordId) throws DatabaseConnectionException,
			RecordExistsException {
		if (localIdDao.searchActive(providerId, recordId).size() > 0) {
			throw new RecordExistsException();
		}
		String id = Base36.encode("/" + providerId + "/" + recordId);
		List<CloudId> cloudIds = cloudIdDao.insert(id, providerId, recordId);
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
	public CloudId getGlobalId(String providerId, String recordId) throws DatabaseConnectionException,
			RecordDoesNotExistException {
		List<CloudId> cloudIds = localIdDao.searchActive(providerId, recordId);
		if (cloudIds.size() == 0) {
			throw new RecordDoesNotExistException();
		}
		return cloudIds.get(0);
	}

	@Override
	public List<LocalId> getLocalIdsByGlobalId(String globalId) throws DatabaseConnectionException,
			GlobalIdDoesNotExistException {
		List<CloudId> cloudIds = cloudIdDao.searchActive(globalId);
		if (cloudIds.size() == 0) {
			throw new GlobalIdDoesNotExistException();
		}
		List<LocalId> localIds = new ArrayList<>();
		for (CloudId cloudId : cloudIds) {
			if (localIdDao.searchActive(cloudId.getLocalId().getProviderId(), cloudId.getLocalId().getRecordId())
					.size() > 0) {
				localIds.add(cloudId.getLocalId());
			}
		}
		return localIds;

	}

	@Override
	public List<LocalId> getLocalIdsByProvider(String providerId, String start, int end)
			throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException {

		List<CloudId> cloudIds = null;
		if (start == null) {
			cloudIds = localIdDao.searchActive(providerId);
		} else {
			cloudIds = localIdDao.searchActiveWithPagination(start, end, providerId);
		}
		List<LocalId> localIds = new ArrayList<>();
		for (CloudId cloudId : cloudIds) {
			localIds.add(cloudId.getLocalId());
		}
		return localIds;
	}

	@Override
	public List<CloudId> getGlobalIdsByProvider(String providerId, String start, int end)
			throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException {

		if (start == null) {
			return localIdDao.searchActive(providerId);
		} else {
			return localIdDao.searchActiveWithPagination(start, end, providerId);
		}
	}

	@Override
	public void createIdMapping(String globalId, String providerId, String recordId)
			throws DatabaseConnectionException, GlobalIdDoesNotExistException, IdHasBeenMappedException {
		List<CloudId> localIds = localIdDao.searchActive(providerId, recordId);
		if (localIds.size() != 0) {
			throw new IdHasBeenMappedException();
		}
		List<CloudId> cloudIds = cloudIdDao.searchActive(globalId);
		if (cloudIds.size() == 0) {
			throw new GlobalIdDoesNotExistException();
		}

		localIdDao.insert(providerId, recordId, globalId);
		cloudIdDao.insert(globalId, providerId, recordId);

	}

	@Override
	public void removeIdMapping(String providerId, String recordId) throws DatabaseConnectionException,
			ProviderDoesNotExistException, RecordIdDoesNotExistException {
		localIdDao.delete(providerId, recordId);

	}

	@Override
	public void deleteGlobalId(String globalId) throws DatabaseConnectionException, GlobalIdDoesNotExistException {

		if (!(cloudIdDao.searchActive(globalId).size() > 0)) {
			throw new GlobalIdDoesNotExistException();
		}
		List<CloudId> localIds = cloudIdDao.searchAll(globalId);
		for (CloudId cId : localIds) {
			localIdDao.delete(cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
			cloudIdDao.delete(globalId, cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
		}

	}

	@Override
	public String getHost() {
		return this.host;
	}

	@Override
	public String getKeyspace() {
		return this.keyspace;
	}

	@Override
	public String getPort() {
		return this.port;
	}

}
