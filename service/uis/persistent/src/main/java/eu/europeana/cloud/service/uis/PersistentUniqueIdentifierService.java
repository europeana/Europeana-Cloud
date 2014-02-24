package eu.europeana.cloud.service.uis;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Service;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.uis.database.dao.CassandraDataProviderDAO;
import eu.europeana.cloud.service.uis.database.dao.CloudIdDao;
import eu.europeana.cloud.service.uis.database.dao.LocalIdDao;
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
 * Cassandra implementation of the Unique Identifier Service
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
@Service
public class PersistentUniqueIdentifierService implements
		UniqueIdentifierService {

	private CloudIdDao cloudIdDao;
	private LocalIdDao localIdDao;
	private CassandraDataProviderDAO dataProviderDao;
	private String host;
	private String keyspace;
	private String port;

	
	/**
	 * Initialization of the service with its DAOs
	 * 
	 * @param cloudIdDao
	 *            The cloud identifier Dao
	 * @param localIdDao
	 *            The local identifier Dao
	 * @param dataProviderDao 
	 */
	public PersistentUniqueIdentifierService(CloudIdDao cloudIdDao,
			LocalIdDao localIdDao,CassandraDataProviderDAO dataProviderDao) {
		this.cloudIdDao = cloudIdDao;
		this.localIdDao = localIdDao;
		this.dataProviderDao = dataProviderDao;
		this.host = cloudIdDao.getHost();
		this.keyspace = cloudIdDao.getKeyspace();
		this.port = cloudIdDao.getPort();
	}

	@Override
	public CloudId createCloudId(String... recordInfo)
			throws DatabaseConnectionException, RecordExistsException,
			ProviderDoesNotExistException, RecordDatasetEmptyException,
			CloudIdDoesNotExistException {
		String providerId = recordInfo[0];
		if(dataProviderDao.getProvider(providerId)==null){
			throw new ProviderDoesNotExistException(
					new IdentifierErrorInfo(
							IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
									.getHttpCode(),
							IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
									.getErrorInfo(providerId)));
		}
		String recordId = recordInfo.length > 1 ? recordInfo[1] : Base36
				.timeEncode(providerId);
		if (!localIdDao.searchActive(providerId, recordId).isEmpty()) {
			throw new RecordExistsException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.RECORD_EXISTS.getHttpCode(),
					IdentifierErrorTemplate.RECORD_EXISTS.getErrorInfo(
							providerId, recordId)));
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
	public CloudId getCloudId(String providerId, String recordId)
			throws DatabaseConnectionException, RecordDoesNotExistException,
			ProviderDoesNotExistException, RecordDatasetEmptyException {
		List<CloudId> cloudIds = localIdDao.searchActive(providerId, recordId);
		if (cloudIds.isEmpty()) {
			throw new RecordDoesNotExistException(
					new IdentifierErrorInfo(
							IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST
									.getHttpCode(),
							IdentifierErrorTemplate.RECORD_DOES_NOT_EXIST
									.getErrorInfo(providerId, recordId)));
		}
		return cloudIds.get(0);
	}

	@Override
	public List<CloudId> getLocalIdsByCloudId(String cloudId)
			throws DatabaseConnectionException, CloudIdDoesNotExistException,
			ProviderDoesNotExistException, RecordDatasetEmptyException {
		List<CloudId> cloudIds = cloudIdDao.searchActive(cloudId);
		if (cloudIds.isEmpty()) {
			throw new CloudIdDoesNotExistException(
					new IdentifierErrorInfo(
							IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST
									.getHttpCode(),
							IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST
									.getErrorInfo(cloudId)));
		}
		List<CloudId> localIds = new ArrayList<>();
		for (CloudId cId : cloudIds) {
			if (localIdDao.searchActive(cId.getLocalId().getProviderId(),
					cId.getLocalId().getRecordId()).size() > 0) {
				localIds.add(cId);
			}
		}
		return localIds;

	}

	@Override
	public List<CloudId> getLocalIdsByProvider(String providerId, String start,
			int end) throws DatabaseConnectionException,
			ProviderDoesNotExistException, RecordDatasetEmptyException {

		if(dataProviderDao.getProvider(providerId)==null){
			throw new ProviderDoesNotExistException(
					new IdentifierErrorInfo(
							IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
									.getHttpCode(),
							IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
									.getErrorInfo(providerId)));
		}
		List<CloudId> cloudIds = null;
		if (start == null) {
			cloudIds = localIdDao.searchActive(providerId);
		} else {
			cloudIds = localIdDao.searchActiveWithPagination(start, end,
					providerId);
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

		if(dataProviderDao.getProvider(providerId)==null){
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
			ProviderDoesNotExistException, RecordDatasetEmptyException {
		if(dataProviderDao.getProvider(providerId)==null){
			throw new ProviderDoesNotExistException(
					new IdentifierErrorInfo(
							IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
									.getHttpCode(),
							IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
									.getErrorInfo(providerId)));
		}
		
		List<CloudId> cloudIds = cloudIdDao.searchActive(cloudId);
		if (cloudIds.isEmpty()) {
			throw new CloudIdDoesNotExistException(
					new IdentifierErrorInfo(
							IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST
									.getHttpCode(),
							IdentifierErrorTemplate.CLOUDID_DOES_NOT_EXIST
									.getErrorInfo(cloudId)));
		}
		List<CloudId> localIds = localIdDao.searchActive(providerId, recordId);
		if (!localIds.isEmpty()) {
			throw new IdHasBeenMappedException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getHttpCode(),
					IdentifierErrorTemplate.ID_HAS_BEEN_MAPPED.getErrorInfo(
							providerId, recordId, cloudId)));
		}

		localIdDao.insert(providerId, recordId, cloudId);
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
	public void removeIdMapping(String providerId, String recordId)
			throws DatabaseConnectionException, ProviderDoesNotExistException,
			RecordIdDoesNotExistException {
		if(dataProviderDao.getProvider(providerId)==null){
			throw new ProviderDoesNotExistException(
					new IdentifierErrorInfo(
							IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
									.getHttpCode(),
							IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST
									.getErrorInfo(providerId)));
		}
		localIdDao.delete(providerId, recordId);

	}

	@Override
	public void deleteCloudId(String cloudId)
			throws DatabaseConnectionException, CloudIdDoesNotExistException {

		if (cloudIdDao.searchActive(cloudId).isEmpty()) {
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

	@Override
	public CloudId createIdMapping(String cloudId, String providerId) throws DatabaseConnectionException,
			CloudIdDoesNotExistException, IdHasBeenMappedException, ProviderDoesNotExistException,
			RecordDatasetEmptyException {
		return createIdMapping(cloudId, providerId, Base36.timeEncode(providerId));
	}

}
