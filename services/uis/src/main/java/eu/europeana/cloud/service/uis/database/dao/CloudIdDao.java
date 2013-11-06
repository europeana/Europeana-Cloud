package eu.europeana.cloud.service.uis.database.dao;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.exceptions.DatabaseConnectionException;
import eu.europeana.cloud.exceptions.GlobalIdDoesNotExistException;
import eu.europeana.cloud.service.uis.database.Dao;
import eu.europeana.cloud.service.uis.database.DatabaseService;

public class CloudIdDao implements Dao<CloudId, List<CloudId>> {

	private String host;
	private String keyspaceName;
	private static String insertStatement = "INSERT INTO Cloud_Id(cloud_id,provider_id,record_id,deleted) VALUES(?,?,?,false)";
	private static String searchStatement = "SELECT * FROM Cloud_Id WHERE cloud_id=? AND deleted=?";
	private static String deleteStatement = "UPDATE Cloud_Id SET deleted=true WHERE cloud_Id=?";
	
	public CloudIdDao(String host, String keyspaceName){
		this.host = host;
		this.keyspaceName = keyspaceName;
	}
	
	@Override
	public List<CloudId> searchById(boolean deleted, String... args) throws DatabaseConnectionException {
		try {
			PreparedStatement statement = DatabaseService.getSession(host, keyspaceName).prepare(searchStatement);
			ResultSet rs = DatabaseService.getSession(host, keyspaceName).execute(statement.bind(args[0], deleted));
			if (!rs.iterator().hasNext()) {
				throw new GlobalIdDoesNotExistException();
			}

			while (!rs.isFullyFetched()) {
				rs.fetchMoreResults();
			}
			List<Row> results = rs.all();
			List<CloudId> cloudIds = new ArrayList<>();
			for (Row row : results) {
				CloudId cId = new CloudId();
				cId.setId(args[0]);
				LocalId lId = new LocalId();
				lId.setProviderId(row.getString("provider_Id"));
				lId.setRecordId(row.getString("record_Id"));
				cId.setLocalId(lId);
				cloudIds.add(cId);
			}
			return cloudIds;
		} catch (NoHostAvailableException e) {
			throw new DatabaseConnectionException();
		}
	}

	@Override
	public List<CloudId> searchActive(String... args) throws DatabaseConnectionException {
		return searchById(false, args[0]);
	}

	@Override
	public List<CloudId> insert(String... args) throws DatabaseConnectionException {
		try {
			PreparedStatement statement = DatabaseService.getSession(host, keyspaceName).prepare(insertStatement);
			DatabaseService.getSession(host, keyspaceName).execute(statement.bind(args[0], args[1], args[2]));
		} catch (NoHostAvailableException e) {
			throw new DatabaseConnectionException();
		}
		return searchActive(args[0]);
	}

	@Override
	public void delete(String... args) throws DatabaseConnectionException {
		try{
			PreparedStatement statement = DatabaseService.getSession(host, keyspaceName).prepare(deleteStatement);
			DatabaseService.getSession(host, keyspaceName).execute(statement.bind(args[0]));
		} catch (NoHostAvailableException e){
			throw new DatabaseConnectionException();
		}
	}

	@Override
	public void update(String... obj) throws DatabaseConnectionException {
		throw new UnsupportedOperationException("This method is not implemented for the Cloud Id");
	}

}
