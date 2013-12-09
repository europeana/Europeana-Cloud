package eu.europeana.cloud.service.uis;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.exceptions.DatabaseConnectionException;
import eu.europeana.cloud.exceptions.CloudIdDoesNotExistException;
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
	public CloudId createCloudId(String ... recordInfo) throws DatabaseConnectionException,
			RecordExistsException {
		String providerId = recordInfo[0];
		String recordId = recordInfo.length>1?recordInfo[1]: Base36.timeEncode(providerId);
		String cloudId = Base36.encode(String.format("/%s/%s", providerId, recordId));
		if (localIdDao.searchActive(providerId, recordId).size() > 0) {
			throw new RecordExistsException();
		}
		localIdDao.insert(cloudId, providerId, recordId);
		return cloudIdDao.insert(cloudId, providerId, recordId).get(0);
	}

	@Override
	public CloudId getCloudId(String providerId, String recordId) throws DatabaseConnectionException,
			RecordDoesNotExistException {
		List<CloudId> cloudIds = localIdDao.searchActive(providerId, recordId);
		if (cloudIds.size() == 0) {
			throw new RecordDoesNotExistException();
		}
		return cloudIds.get(0);
	}

	@Override
	public List<LocalId> getLocalIdsByCloudId(String cloudId) throws DatabaseConnectionException,
			CloudIdDoesNotExistException {
		List<CloudId> cloudIds = cloudIdDao.searchActive(cloudId);
		if (cloudIds.size() == 0) {
			throw new CloudIdDoesNotExistException();
		}
		List<LocalId> localIds = new ArrayList<>();
		for (CloudId cId : cloudIds) {
			if (localIdDao.searchActive(cId.getLocalId().getProviderId(), cId.getLocalId().getRecordId())
					.size() > 0) {
				localIds.add(cId.getLocalId());
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
	public List<CloudId> getCloudIdsByProvider(String providerId, String start, int end)
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
	public void createIdMapping(String cloudId, String providerId, String recordId)
			throws DatabaseConnectionException, CloudIdDoesNotExistException, IdHasBeenMappedException,ProviderDoesNotExistException {
		List<CloudId> localIds = localIdDao.searchActive(providerId, recordId);
		if (localIds.size() != 0) {
			throw new IdHasBeenMappedException();
		}
		List<CloudId> cloudIds = cloudIdDao.searchActive(cloudId);
		if (cloudIds.size() == 0) {
			throw new CloudIdDoesNotExistException();
		}

		localIdDao.insert(cloudId, providerId, recordId);
		cloudIdDao.insert(cloudId, providerId, recordId);
	}

	@Override
	public void removeIdMapping(String providerId, String recordId) throws DatabaseConnectionException,
			ProviderDoesNotExistException, RecordIdDoesNotExistException {

		localIdDao.delete(providerId, recordId);

	}

	@Override
	public void deleteCloudId(String cloudId) throws DatabaseConnectionException, CloudIdDoesNotExistException {
		if (!(cloudIdDao.searchActive(cloudId).size() > 0)) {
			throw new CloudIdDoesNotExistException();
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

	public void reset() {
		localIdDao.reset();
		cloudIdDao.reset();
	}
}
