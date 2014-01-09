package eu.europeana.cloud.service.uis.database.dao;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.service.uis.Dao;
import eu.europeana.cloud.service.uis.database.DatabaseService;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;

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
	private PreparedStatement insertStatement;
	private PreparedStatement deleteStatement;
	private PreparedStatement updateStatement;
	private PreparedStatement searchByProviderStatement; 
	private PreparedStatement searchByRecordIdStatement;
	private PreparedStatement searchByProviderPaginatedStatement;
	
	

	/**
	 * The LocalId Dao
	 * @param dbService The service that exposes the database connection
	 */
	public LocalIdDao(DatabaseService dbService) {
		this.dbService = dbService;
		this.host = dbService.getHost();
		this.port = dbService.getPort();
		this.keyspaceName = dbService.getKeyspaceName();
		prepareStatements();
	}

	private void prepareStatements(){
		insertStatement = dbService.getSession().prepare("INSERT INTO Provider_Record_Id(provider_id,record_id,cloud_id,deleted) VALUES(?,?,?,false)");
		insertStatement.setConsistencyLevel(dbService.getConsistencyLevel());
		deleteStatement = dbService.getSession().prepare("UPDATE Provider_Record_Id SET deleted=true WHERE provider_id=? AND record_Id=?");
		deleteStatement.setConsistencyLevel(dbService.getConsistencyLevel());
		updateStatement = dbService.getSession().prepare("UPDATE Provider_Record_Id SET cloud_id=? WHERE provider_id=? AND record_Id=?");
		updateStatement.setConsistencyLevel(dbService.getConsistencyLevel());
		searchByProviderStatement = dbService.getSession().prepare("SELECT * FROM Provider_Record_Id WHERE provider_id=? AND deleted = ? ALLOW FILTERING");
		searchByProviderStatement.setConsistencyLevel(dbService.getConsistencyLevel());
		searchByRecordIdStatement = dbService.getSession().prepare("SELECT * FROM Provider_Record_Id WHERE provider_id=? AND record_id=? AND deleted=? ALLOW FILTERING");
		searchByRecordIdStatement.setConsistencyLevel(dbService.getConsistencyLevel());
		searchByProviderPaginatedStatement = dbService.getSession().prepare("SELECT * FROM Provider_Record_Id WHERE provider_id=? AND record_id>=? LIMIT ? ALLOW FILTERING");
		searchByProviderPaginatedStatement.setConsistencyLevel(dbService.getConsistencyLevel());
	}
	@Override
	public List<CloudId> searchById(boolean deleted, String... args) throws DatabaseConnectionException,
			ProviderDoesNotExistException, RecordDatasetEmptyException {
		try {
			ResultSet rs = null;
			
			if (args.length == 1) {
				rs = dbService.getSession().execute(searchByProviderStatement.bind(args[0], deleted));

			} else if (args.length >= 2) {
				rs = dbService.getSession().execute(searchByRecordIdStatement.bind(args[0], args[1], deleted));
			}
//			while (rs!=null && !rs.isFullyFetched()) {
//				rs.fetchMoreResults();
//			}
			return createCloudIdsFromRs(rs);
		} catch (NoHostAvailableException e) {
			throw new DatabaseConnectionException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
					IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(host,port,e.getMessage())));
		}
	}

	@Override
	public List<CloudId> searchActive(String... args) throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException {
		return searchById(false, args);
	}

	/**
	 * Enable pagination search on active local Id information
	 * @param start Record to start from
	 * @param end The number of record to retrieve
	 * @param providerId The provider Identifier
	 * @return A list of CloudId objects
	 */
	public List<CloudId> searchActiveWithPagination(String start, int end, String providerId) {
		ResultSet rs = dbService.getSession().execute(searchByProviderPaginatedStatement.bind(providerId, start, end));
//		while (!rs.isFullyFetched()) {
//			rs.fetchMoreResults();
//		}
		return createCloudIdsFromRs(rs);
	}

	@Override
	public List<CloudId> insert(String... args) throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException {
		try {

			dbService.getSession().execute(insertStatement.bind(args[0], args[1], args[2]));
		} catch (NoHostAvailableException e) {
			throw new DatabaseConnectionException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
					IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(host,port,e.getMessage())));
		}
		List<CloudId> cIds = new ArrayList<>();
		CloudId cId = new CloudId();
		LocalId lId = new LocalId();
		lId.setProviderId(args[0]);
		lId.setRecordId(args[1]);
		cId.setLocalId(lId);
		cId.setId(args[2]);
		cIds.add(cId);
		return cIds;
	}

	@Override
	public void delete(String... args) throws DatabaseConnectionException {
		try {
			dbService.getSession().execute(deleteStatement.bind(args[0], args[1]));
		} catch (NoHostAvailableException e) {
			throw new DatabaseConnectionException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
					IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(host,port,e.getMessage())));
		}
	}

	@Override
	public void update(String... args) throws DatabaseConnectionException {
		try {
			dbService.getSession().execute(updateStatement.bind(args[0], args[1], args[2]));
		} catch (NoHostAvailableException e) {
			throw new DatabaseConnectionException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getHttpCode(),
					IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR.getErrorInfo(host,port,e.getMessage())));
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
