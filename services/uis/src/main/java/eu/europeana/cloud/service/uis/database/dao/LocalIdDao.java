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
import eu.europeana.cloud.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordDatasetEmptyException;
import eu.europeana.cloud.exceptions.RecordIdDoesNotExistException;
import eu.europeana.cloud.service.uis.database.Dao;
import eu.europeana.cloud.service.uis.database.DatabaseService;

public class LocalIdDao implements Dao<CloudId, List<CloudId>> {

	private String host;
	private String keyspaceName;

	private static String insertStatement = "INSERT INTO Provider_Record_Id(provider_id,record_id,cloud_id,deleted) VALUES(?,?,?,false)";
	private static String deleteStatement = "UPDATE Provider_Record_Id SET deleted=true WHERE provider_id=? AND record_Id=?";
	private static String updateStatement = "UPDATE Provider_Record_Id SET cloud_id=? where provider_id=? AND record_Id=? AND deleted=false";
	private static String searchByProviderStatement = "SELECT * FROM Provider_Record_Id WHERE provider_id=? AND deleted = ?";
	private static String searchByRecordIdStatement = "SELECT * FROM Provider_Record_Id WHERE provider_id=? AND record_id=? AND deleted=?";

	public LocalIdDao(String host, String keyspaceName) {
		this.host = host;
		this.keyspaceName = keyspaceName;
	}

	@Override
	public List<CloudId> searchById(boolean deleted, String... args) throws DatabaseConnectionException,
			ProviderDoesNotExistException, RecordDatasetEmptyException {
		try {
			PreparedStatement statement = null;
			ResultSet rs = null;
			if (args.length == 1) {
				statement = DatabaseService.getSession(host, keyspaceName).prepare(searchByProviderStatement);
				rs = DatabaseService.getSession(host, keyspaceName).execute(statement.bind(args[0],deleted));
				
			} else if (args.length == 2) {
				statement = DatabaseService.getSession(host, keyspaceName).prepare(searchByRecordIdStatement);
				rs = DatabaseService.getSession(host, keyspaceName).execute(statement.bind(args[0], args[1],deleted));
				
			}

			List<CloudId> cloudIds = new ArrayList<>();
			while (!rs.isFullyFetched()) {
				rs.fetchMoreResults();
			}
			for (Row row : rs.all()) {
				LocalId lId = new LocalId();
				lId.setProviderId(row.getString("provider_Id"));
				lId.setRecordId(row.getString("record_Id"));
				CloudId cloudId = new CloudId();
				cloudId.setId(row.getString("cloud_id"));
				cloudId.setLocalId(lId);
				cloudIds.add(cloudId);
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
		return searchActive(args);
	}

	@Override
	public void delete(String... args) throws DatabaseConnectionException {
		try {
			PreparedStatement statement = DatabaseService.getSession(host, keyspaceName).prepare(deleteStatement);
			DatabaseService.getSession(host, keyspaceName).execute(statement.bind(args[0], args[1]));
		} catch (NoHostAvailableException e) {
			throw new DatabaseConnectionException();
		}
	}

	@Override
	public void update(String... args) throws DatabaseConnectionException {
		try {
			PreparedStatement statement = DatabaseService.getSession(host, keyspaceName).prepare(updateStatement);
			DatabaseService.getSession(host, keyspaceName).execute(statement.bind(args[0], args[1], args[2]));
		} catch (NoHostAvailableException e) {
			throw new DatabaseConnectionException();
		}
	}

}
