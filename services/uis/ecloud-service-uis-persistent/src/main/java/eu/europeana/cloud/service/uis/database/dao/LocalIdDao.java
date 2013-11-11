package eu.europeana.cloud.service.uis.database.dao;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.exceptions.DatabaseConnectionException;
import eu.europeana.cloud.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.exceptions.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.Dao;
import eu.europeana.cloud.service.uis.database.DatabaseService;

/**
 * Dao providing access to the search based on record id and provider id
 * operations
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public class LocalIdDao implements Dao<CloudId, List<CloudId>> {

	private String host;
	private String port;
	private String keyspaceName;
	private DatabaseService dbService;
	private static String insertStatement = "INSERT INTO Provider_Record_Id(provider_id,record_id,cloud_id,deleted) VALUES(?,?,?,false)";
	private static String deleteStatement = "UPDATE Provider_Record_Id SET deleted=true WHERE provider_id=? AND record_Id=?";
	private static String updateStatement = "UPDATE Provider_Record_Id SET cloud_id=? where provider_id=? AND record_Id=? AND deleted=false";
	private static String searchByProviderStatement = "SELECT * FROM Provider_Record_Id WHERE provider_id=? AND deleted = ? ALLOW FILTERING";
	private static String searchByRecordIdStatement = "SELECT * FROM Provider_Record_Id WHERE provider_id=? AND record_id=? AND deleted=? ALLOW FILTERING";
	private static String searchByProviderPaginatedStatement = "SELECT * FROM Provider_Record_Id WHERE provider_id=? AND record_id>? LIMIT ? ALLOW FILTERING";

	public LocalIdDao(DatabaseService dbService) {
		this.dbService = dbService;
		this.host = dbService.getHost();
		this.port = dbService.getPort();
		this.keyspaceName = dbService.getKeyspaceName();
	}

	@Override
	public List<CloudId> searchById(boolean deleted, String... args) throws DatabaseConnectionException,
			ProviderDoesNotExistException, RecordDatasetEmptyException {
		try {
			PreparedStatement statement = null;
			ResultSet rs = null;
			if (args.length == 1) {
				statement = dbService.getSession().prepare(searchByProviderStatement);
				rs = dbService.getSession().execute(statement.bind(args[0], deleted));

			} else if (args.length >= 2) {
				statement = dbService.getSession().prepare(searchByRecordIdStatement);
				rs = dbService.getSession().execute(statement.bind(args[0], args[1], deleted));

			}
			while (!rs.isFullyFetched()) {
				rs.fetchMoreResults();
			}
			return createCloudIdsFromRs(rs);
		} catch (NoHostAvailableException e) {
			throw new DatabaseConnectionException();
		}
	}

	@Override
	public List<CloudId> searchActive(String... args) throws DatabaseConnectionException {
		return searchById(false, args);
	}

	public List<CloudId> searchActiveWithPagination(String start, int end, String... args) {
		PreparedStatement statement = dbService.getSession().prepare(searchByProviderPaginatedStatement);
		ResultSet rs = dbService.getSession().execute(statement.bind(args[0], start, end));
		while (!rs.isFullyFetched()) {
			rs.fetchMoreResults();
		}
		return createCloudIdsFromRs(rs);
	}

	@Override
	public List<CloudId> insert(String... args) throws DatabaseConnectionException {
		try {

			PreparedStatement statement = dbService.getSession().prepare(insertStatement);
			dbService.getSession().execute(statement.bind(args[0], args[1], args[2]));
		} catch (NoHostAvailableException e) {
			throw new DatabaseConnectionException();
		}

		return searchActive(args);
	}

	@Override
	public void delete(String... args) throws DatabaseConnectionException {
		try {
			PreparedStatement statement = dbService.getSession().prepare(deleteStatement);
			dbService.getSession().execute(statement.bind(args[0], args[1]));
		} catch (NoHostAvailableException e) {
			throw new DatabaseConnectionException();
		}
	}

	@Override
	public void update(String... args) throws DatabaseConnectionException {
		try {
			PreparedStatement statement = dbService.getSession().prepare(updateStatement);
			dbService.getSession().execute(statement.bind(args[0], args[1], args[2]));
		} catch (NoHostAvailableException e) {
			throw new DatabaseConnectionException();
		}
	}

	@Override
	public String getHost() {
		return host;
	}

	@Override
	public String getKeyspace() {
		return keyspaceName;
	}

	private List<CloudId> createCloudIdsFromRs(ResultSet rs) {
		List<CloudId> cloudIds = new ArrayList<>();
		for (Row row : rs.all()) {
			if (!row.getBool("deleted")) {
				LocalId lId = new LocalId();
				lId.setProviderId(row.getString("provider_Id"));
				lId.setRecordId(row.getString("record_Id"));
				CloudId cloudId = new CloudId();
				cloudId.setId(row.getString("cloud_id"));
				cloudId.setLocalId(lId);
				cloudIds.add(cloudId);
			}
		}
		return cloudIds;
	}

	@Override
	public String getPort() {
		return this.port;
	}
}
