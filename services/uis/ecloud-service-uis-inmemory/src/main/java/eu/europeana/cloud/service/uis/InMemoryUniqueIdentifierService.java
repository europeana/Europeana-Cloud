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
import eu.europeana.cloud.service.uis.dao.InMemoryCloudIdDao;
import eu.europeana.cloud.service.uis.dao.InMemoryLocalIdDao;
import eu.europeana.cloud.service.uis.encoder.Base36;

/**
 * In-memory mockup of the unique identifier service
 * 
 * @see UniqueIdentifierService
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 17, 2013
 */
@Service
public class InMemoryUniqueIdentifierService implements UniqueIdentifierService {

	private InMemoryCloudIdDao cloudIdDao = new InMemoryCloudIdDao();
	private InMemoryLocalIdDao localIdDao = new InMemoryLocalIdDao();

	@Override
	public CloudId createGlobalId(String providerId, String recordId) throws DatabaseConnectionException,
			RecordExistsException {
		String globalId = Base36.encode(String.format("/%s/%s", providerId, recordId));
		if (localIdDao.searchActive(providerId, recordId).size() > 0) {
			throw new RecordExistsException();
		}
		localIdDao.insert(globalId, providerId, recordId);
		return cloudIdDao.insert(globalId, providerId, recordId).get(0);
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
			throws DatabaseConnectionException, ProviderDoesNotExistException {
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
		List<CloudId> cloudIds;

		if (start == null) {
			cloudIds = localIdDao.searchActive(providerId);
		} else {
			cloudIds = localIdDao.searchActiveWithPagination(start, end, providerId);
		}

		if (cloudIds.size() == 0) {
			throw new RecordDatasetEmptyException();
		}
		return cloudIds;
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

		localIdDao.insert(globalId, providerId, recordId);
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
		List<CloudId> localIds = cloudIdDao.searchActive(globalId);
		for (CloudId cId : localIds) {
			localIdDao.delete(cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
			cloudIdDao.delete(globalId, cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId());
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

	public void reset() {
		localIdDao.reset();
		cloudIdDao.reset();
	}
}
