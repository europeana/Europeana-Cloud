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
import eu.europeana.cloud.service.uis.Dao;
import eu.europeana.cloud.service.uis.database.DatabaseService;

/**
 * Dao providing access to database operations on Cloud id database
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class CloudIdDao implements Dao<CloudId, List<CloudId>> {

	private String host;
	private String keyspaceName;
	private String port;
	private DatabaseService dbService;
	private static String insertStatement = "INSERT INTO Cloud_Id(cloud_id,provider_id,record_id,deleted) VALUES(?,?,?,false)";
	private static String searchStatement = "SELECT * FROM Cloud_Id WHERE cloud_id=? AND deleted=?";
	private static String searchStatementNonActive = "SELECT * FROM Cloud_Id WHERE cloud_id=?";
	private static String deleteStatement = "UPDATE Cloud_Id SET deleted=true WHERE cloud_Id=? AND provider_id=? AND record_id=?";
	
	public CloudIdDao(DatabaseService dbService){
		this.dbService = dbService;
		this.host = dbService.getHost();
		this.port = dbService.getPort();
		this.keyspaceName = dbService.getKeyspaceName();
	}
	
	@Override
	public List<CloudId> searchById(boolean deleted, String... args) throws DatabaseConnectionException {
		try {
			PreparedStatement statement = dbService.getSession().prepare(searchStatement);
			ResultSet rs = dbService.getSession().execute(statement.bind(args[0], deleted));
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

	public List<CloudId> searchAll(String args) throws DatabaseConnectionException {
		PreparedStatement statement = dbService.getSession().prepare(searchStatementNonActive);
		ResultSet rs = dbService.getSession().execute(statement.bind(args));
		List<Row> results = rs.all();
		List<CloudId> cloudIds = new ArrayList<>();
		for (Row row : results) {
			CloudId cId = new CloudId();
			cId.setId(args);
			LocalId lId = new LocalId();
			lId.setProviderId(row.getString("provider_Id"));
			lId.setRecordId(row.getString("record_Id"));
			cId.setLocalId(lId);
			cloudIds.add(cId);
		}
		return cloudIds;
	}
	
	@Override
	public List<CloudId> insert(String... args) throws DatabaseConnectionException {
		try {
			PreparedStatement statement = dbService.getSession().prepare(insertStatement);
			dbService.getSession().execute(statement.bind(args[0], args[1], args[2]));
		} catch (NoHostAvailableException e) {
			throw new DatabaseConnectionException();
		}
		return searchActive(args[0]);
	}

	@Override
	public void delete(String... args) throws DatabaseConnectionException {
		try{
			PreparedStatement statement = dbService.getSession().prepare(deleteStatement);
			dbService.getSession().execute(statement.bind(args[0],args[1],args[2]));
		} catch (NoHostAvailableException e){
			throw new DatabaseConnectionException();
		}
	}

	@Override
	public void update(String... obj) throws DatabaseConnectionException {
		throw new UnsupportedOperationException("This method is not implemented for the Cloud Id");
	}

	@Override
	public String getHost() {
		return host;
	}

	@Override
	public String getKeyspace() {
		return keyspaceName;
	}

	@Override
	public String getPort() {
		return this.port;
	}

	
}
