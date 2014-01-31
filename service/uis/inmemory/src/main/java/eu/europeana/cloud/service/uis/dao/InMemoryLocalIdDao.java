package eu.europeana.cloud.service.uis.dao;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.uis.Dao;
import eu.europeana.cloud.service.uis.InMemoryCloudObject;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
/**
 * In Memory implementation of the Local Id DAO
 * 
 * 
 * @author Yorgos Mamakis (Yorgos.Mamakis@ europeana.eu)
 * @since Dec 20, 2013
 */
public class InMemoryLocalIdDao implements Dao<CloudId, List<CloudId>> {

	private static List<InMemoryCloudObject> cloudIds = new ArrayList<>();

	@Override
	public List<CloudId> searchById(boolean deleted, String... args) throws DatabaseConnectionException,ProviderDoesNotExistException {
		List<CloudId> retCloudIds = new ArrayList<>();
		if (args.length == 1) {
			for (InMemoryCloudObject obj : cloudIds) {
				if (obj.getProviderId().contentEquals(args[0]) && obj.isDeleted() == deleted) {
					CloudId cId = new CloudId();
					cId.setId(obj.getCloudId());
					LocalId lId = new LocalId();
					lId.setProviderId(obj.getProviderId());
					lId.setRecordId(obj.getRecordId());
					cId.setLocalId(lId);
					retCloudIds.add(cId);
				}
			}
			if (retCloudIds.isEmpty()) {
				throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
						IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
						IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(args[0])));
			}
		}

		if (args.length == 2) {
			for (InMemoryCloudObject obj : cloudIds) {
				if (obj.getProviderId().contentEquals(args[0]) && obj.getRecordId().contentEquals(args[1])
						&& obj.isDeleted() == deleted) {
					CloudId cId = new CloudId();
					cId.setId(obj.getCloudId());
					LocalId lId = new LocalId();
					lId.setProviderId(obj.getProviderId());
					lId.setRecordId(obj.getRecordId());
					cId.setLocalId(lId);
					retCloudIds.add(cId);
				}
			}
		}

		return retCloudIds;
	}

	@Override
	public List<CloudId> searchActive(String... args) throws DatabaseConnectionException, ProviderDoesNotExistException {
		return searchById(false, args);
	}

	/**
	 * Method that enables result pagination of search requests
	 * 
	 * @param start
	 *            The record to start from
	 * @param end
	 *            How many records to retrieve
	 * @param providerId
	 *            The provider Identifier to search on
	 * @return A list of Cloud Identifiers that conforms to the search criteria
	 * @throws ProviderDoesNotExistException 
	 * @throws RecordDatasetEmptyException 
	 */
	public List<CloudId> searchActiveWithPagination(String start, int end, String providerId) throws ProviderDoesNotExistException,RecordDatasetEmptyException{
		List<CloudId> cIds = new ArrayList<>();
		int index = 0;
		int i = 0;
		boolean providerDoesNotExist = true;
		for (InMemoryCloudObject obj : cloudIds) {
			if (obj.getProviderId().contentEquals(providerId)) {
				providerDoesNotExist = false;
				if (obj.getRecordId().contentEquals(start)) {
					index = i;
				}
			}
			i++;
		}
		if (providerDoesNotExist) {
			throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
					IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(providerId)));
		}
		int k = 0;
		for (InMemoryCloudObject obj : cloudIds.subList(index, cloudIds.size())) {
			if (obj.getProviderId().contentEquals(providerId)) {
				if (k == 0 && !obj.getRecordId().contentEquals(start)) {
					break;
				}
				CloudId cId = new CloudId();
				cId.setId(obj.getCloudId());
				LocalId lId = new LocalId();
				lId.setProviderId(providerId);
				lId.setRecordId(obj.getRecordId());
				cIds.add(cId);
				k++;
				if (cIds.size() == end) {

					return cIds;
				}
			}

		}
		if (cIds.size() == 0) {
			throw new RecordDatasetEmptyException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.RECORDSET_EMPTY.getHttpCode(),
					IdentifierErrorTemplate.RECORDSET_EMPTY.getErrorInfo(providerId)));
		}
		return cIds;

	}

	@Override
	public List<CloudId> insert(String... args) throws DatabaseConnectionException {
		InMemoryCloudObject obj = new InMemoryCloudObject();
		obj.setCloudId(args[0]);
		obj.setProviderId(args[1]);
		obj.setRecordId(args[2]);
		obj.setDeleted(false);
		cloudIds.add(obj);
		final CloudId cId = new CloudId();
		cId.setId(args[0]);
		LocalId lId = new LocalId();
		lId.setProviderId(args[1]);
		lId.setRecordId(args[2]);
		cId.setLocalId(lId);
		return ImmutableList.of(cId);
	}

	@Override
	public void delete(String... args) throws DatabaseConnectionException,ProviderDoesNotExistException,RecordIdDoesNotExistException {
		InMemoryCloudObject objNew = new InMemoryCloudObject();
		boolean deleted = false;
		int i = 0;
		int index = 0;
		for (InMemoryCloudObject obj : cloudIds) {
			if (obj.getProviderId().contentEquals(args[0])) {
				objNew.setProviderId(obj.getProviderId());
				if (obj.getRecordId().contentEquals(args[1]) && !obj.isDeleted()) {
					obj.setDeleted(true);
					deleted = true;
					objNew = obj;
					index = i;
				}
			}

			i++;
		}
		if (deleted) {
			cloudIds.remove(index);
			cloudIds.add(objNew);
		}
		
		if(objNew.getProviderId()==null){
			throw new ProviderDoesNotExistException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getHttpCode(),
					IdentifierErrorTemplate.PROVIDER_DOES_NOT_EXIST.getErrorInfo(args[0])));
		}
		if(objNew.getRecordId()==null){
			throw new RecordIdDoesNotExistException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.RECORDID_DOES_NOT_EXIST.getHttpCode(),
					IdentifierErrorTemplate.RECORDID_DOES_NOT_EXIST.getErrorInfo(args[1])));
		}
	}

	@Override
	public void update(String... args) throws DatabaseConnectionException {
		for (InMemoryCloudObject obj : cloudIds) {
			if (obj.getProviderId().contentEquals(args[1]) && obj.getRecordId().contentEquals(args[2])
					&& !obj.isDeleted()) {
				obj.setCloudId(args[0]);
			}
		}
	}

	@Override
	public String getHost() {
		return "";
	}

	@Override
	public String getKeyspace() {
		return "";
	}

	@Override
	public String getPort() {
		return "";
	}

	/**
	 * Clear out LocalId cache
	 */
	public void reset() {
		cloudIds = new ArrayList<>();
	}
}
