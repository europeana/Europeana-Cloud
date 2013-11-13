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

@Service
public class PersistentUniqueIdentifierService implements UniqueIdentifierService {

	private CloudIdDao cloudIdDao;
	private LocalIdDao localIdDao;

	private String host;
	private String keyspace;
	private String port;

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
		try {
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
		} catch (DatabaseConnectionException e) {
			throw e;
		}
	}

	@Override
	public CloudId getGlobalId(String providerId, String recordId) throws DatabaseConnectionException,
			RecordDoesNotExistException {
		try {
			List<CloudId> cloudIds = localIdDao.searchActive(providerId, recordId);
			if (cloudIds.size() == 0) {
				throw new RecordDoesNotExistException();
			}
			return cloudIds.get(0);
		} catch (DatabaseConnectionException e) {
			throw e;
		}
	}

	@Override
	public List<LocalId> getLocalIdsByGlobalId(String globalId) throws DatabaseConnectionException,
			GlobalIdDoesNotExistException {
		try {
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
		} catch (DatabaseConnectionException e) {
			throw e;
		}

	}

	@Override
	public List<LocalId> getLocalIdsByProvider(String providerId, String start, int end)
			throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException {
		try {
			
			List<CloudId> cloudIds = null;
			if(start==null){
				cloudIds = localIdDao.searchActive(providerId);
			} else {
				cloudIds = localIdDao.searchActiveWithPagination(start, end, providerId);
			}
			List<LocalId> localIds = new ArrayList<>();
			for (CloudId cloudId : cloudIds) {
				localIds.add(cloudId.getLocalId());
			}
			return localIds;
		} catch (DatabaseConnectionException e) {
			throw e;
		} catch (ProviderDoesNotExistException e) {
			throw e;
		}
	}

	@Override
	public List<CloudId> getGlobalIdsByProvider(String providerId, String start, int end)
			throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException {
		try {
			
			if(start==null){
				return localIdDao.searchActive(providerId);
			} else {
				return localIdDao.searchActiveWithPagination(start, end, providerId);
			}

		} catch (DatabaseConnectionException e) {
			throw e;
		} catch (ProviderDoesNotExistException e) {
			throw e;
		} catch (RecordDatasetEmptyException e) {
			throw e;
		}
	}

	@Override
	public void createIdMapping(String globalId, String providerId, String recordId)
			throws DatabaseConnectionException, GlobalIdDoesNotExistException, IdHasBeenMappedException {
		try {
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
		} catch (DatabaseConnectionException e) {
			throw e;
		}

	}

	@Override
	public void removeIdMapping(String providerId, String recordId) throws DatabaseConnectionException,
			ProviderDoesNotExistException, RecordIdDoesNotExistException {
		try {
			localIdDao.delete(providerId, recordId);
		} catch (DatabaseConnectionException e) {
			throw e;
		} catch (ProviderDoesNotExistException e) {
			throw e;
		} catch (RecordIdDoesNotExistException e) {
			throw e;
		}

	}

	@Override
	public void deleteGlobalId(String globalId) throws DatabaseConnectionException, GlobalIdDoesNotExistException {
		try {
			if (!(cloudIdDao.searchActive(globalId).size() > 0)) {
				throw new GlobalIdDoesNotExistException();
			}
			List<CloudId> localIds = cloudIdDao.searchAll(globalId);
			for (CloudId cId : localIds) {
				localIdDao.delete(cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
				cloudIdDao.delete(globalId,cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
			}
		} catch (DatabaseConnectionException e) {
			throw e;
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
