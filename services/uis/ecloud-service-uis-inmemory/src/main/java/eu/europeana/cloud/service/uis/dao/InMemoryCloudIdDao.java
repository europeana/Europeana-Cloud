package eu.europeana.cloud.service.uis.dao;

import java.util.ArrayList;
import java.util.List;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.uis.Dao;
import eu.europeana.cloud.service.uis.InMemoryCloudObject;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
/**
 * In Memory Cloud Id implementation
 * 
 * 
 * @author Yorgos Mamakis (Yorgos.Mamakis@ europeana.eu)
 * @since Dec 20, 2013
 */
public class InMemoryCloudIdDao implements Dao<CloudId, List<CloudId>> {

	private static List<InMemoryCloudObject> cloudIds = new ArrayList<>();

	@Override
	public List<CloudId> searchById(boolean deleted, String... args) throws DatabaseConnectionException {
		List<CloudId> retCloudIds = new ArrayList<>();
		
			for (InMemoryCloudObject obj : cloudIds) {
				if (obj.getCloudId().contentEquals(args[0]) && obj.isDeleted() == deleted) {
					CloudId cId = new CloudId();
					cId.setId(obj.getCloudId());
					LocalId lId = new LocalId();
					lId.setProviderId(obj.getProviderId());
					lId.setRecordId(obj.getRecordId());
					cId.setLocalId(lId);
					retCloudIds.add(cId);
				}
			}
		return retCloudIds;
	}

	@Override
	public List<CloudId> searchActive(String... args) throws DatabaseConnectionException {
		return searchById(false, args);
	}

	/**
	 * Method that searches both active and inactive identifiers
	 * @param cloudId The global Identifier to search on
	 * @return A List of Cloud Identifiers
	 * @throws DatabaseConnectionException
	 */
	public List<CloudId> searchAll(String cloudId) throws DatabaseConnectionException{
		List<CloudId> cIds = new ArrayList<>();
		for(InMemoryCloudObject obj:cloudIds){
			if(obj.getCloudId().contentEquals(cloudId)){
				CloudId cId = new CloudId();
				cId.setId(cloudId);
				LocalId lId = new LocalId();
				lId.setProviderId(obj.getProviderId());
				lId.setRecordId(obj.getRecordId());
				cId.setLocalId(lId);
				cIds.add(cId);
			}
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
		return new ArrayList<CloudId>(){
			private static final long serialVersionUID = 4075489743327584853L;

		{
			add(cId);
		}};
	}

	@Override
	public void delete(String... args) throws DatabaseConnectionException {
		for(InMemoryCloudObject obj:cloudIds){
			if(obj.getCloudId().contentEquals(args[0])&&obj.getProviderId().contentEquals(args[1])&&obj.getRecordId().contentEquals(args[2])){
				obj.setDeleted(true);
			}
		}
	}

	@Override
	public void update(String... args) throws DatabaseConnectionException {
		throw new UnsupportedOperationException();
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
	 * Method that empties the Cloud Id cache
	 */
	public void reset(){
		cloudIds = new ArrayList<>();
	}
}
