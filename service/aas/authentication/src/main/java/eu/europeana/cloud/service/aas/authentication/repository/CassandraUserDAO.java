package eu.europeana.cloud.service.aas.authentication.repository;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.collect.ImmutableSet;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;

import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.model.User;
import eu.europeana.cloud.service.aas.authentication.SpringUser;
import eu.europeana.cloud.service.aas.authentication.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.aas.authentication.exception.UserDoesNotExistException;
import eu.europeana.cloud.service.aas.authentication.exception.UserExistsException;
import eu.europeana.cloud.service.aas.authentication.status.IdentifierErrorTemplate;

/**
 * Used to create / retrieve / update <code>User</code> objects from ecloud's
 * database.
 * 
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 */
public class CassandraUserDAO {

    private static final Log LOGGER = LogFactory.getLog(CassandraUserDAO.class);

    private CassandraConnectionProvider provider;

    private PreparedStatement selectUserStatement;
    private PreparedStatement createUserStatement;
    private PreparedStatement updateUserStatement;
    private PreparedStatement deleteUserStatement;

    /**
     * Default roles for all users of ecloud (that don't have admin, or other
     * superpowers)
     */
    private static Set<String> DEFAULT_USER_ROLES = ImmutableSet
	    .of("ROLE_USER");

    /**
     * Constructs a new <code>CassandraUserDAO</code>.
     * 
     * @param provider
     *            the <code>Session</code> to use for connectivity with
     *            Cassandra.
     */
    public CassandraUserDAO(final CassandraConnectionProvider provider) {
	LOGGER.info("CassandraUserDAO starting...");
	this.provider = provider;
	prepareStatements();
	LOGGER.info("CassandraUserDAO started successfully.");
    }

    private void prepareStatements() {
	selectUserStatement = provider.getSession().prepare(
		"SELECT * FROM users WHERE username = ?");
	selectUserStatement.setConsistencyLevel(provider.getConsistencyLevel());

	createUserStatement = provider
		.getSession()
		.prepare(
			"INSERT INTO users(username, password, roles) VALUES (?,?,?) IF NOT EXISTS;");
	createUserStatement.setConsistencyLevel(provider.getConsistencyLevel());

	updateUserStatement = provider.getSession().prepare(
		"UPDATE users SET password = ? WHERE username = ?;");
	updateUserStatement.setConsistencyLevel(provider.getConsistencyLevel());

	deleteUserStatement = provider.getSession().prepare(
		"DELETE FROM users WHERE username = ?;");
	deleteUserStatement.setConsistencyLevel(provider.getConsistencyLevel());
    }

    public SpringUser getUser(final String username)
	    throws DatabaseConnectionException, UserDoesNotExistException {
	try {
	    BoundStatement boundStatement = selectUserStatement.bind(username);
	    ResultSet rs = provider.getSession().execute(boundStatement);
	    Row result = rs.one();
	    if (result == null) {
		throw new UserDoesNotExistException(
			new IdentifierErrorInfo(
				IdentifierErrorTemplate.USER_DOES_NOT_EXIST
					.getHttpCode(),
				IdentifierErrorTemplate.USER_DOES_NOT_EXIST
					.getErrorInfo(username)));

	    }
	    return mapUser(result);
	} catch (NoHostAvailableException e) {
	    throw new DatabaseConnectionException(
		    new IdentifierErrorInfo(
			    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
				    .getHttpCode(),
			    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
				    .getErrorInfo(provider.getHosts(),
					    provider.getPort(), e.getMessage())));
	}
    }

    public void createUser(final User user) throws DatabaseConnectionException,
	    UserExistsException {

	try {
	    if (userAlreadyExistsInDB(user.getUsername())) {
		throw new UserExistsException(new IdentifierErrorInfo(
			IdentifierErrorTemplate.USER_EXISTS.getHttpCode(),
			IdentifierErrorTemplate.USER_EXISTS.getErrorInfo(user
				.getUsername())));
	    } else {
		BoundStatement boundStatement = createUserStatement.bind(
			user.getUsername(), user.getPassword());
		provider.getSession().execute(boundStatement);
	    }
	} catch (NoHostAvailableException e) {
	    throw new DatabaseConnectionException(
		    new IdentifierErrorInfo(
			    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
				    .getHttpCode(),
			    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
				    .getErrorInfo(provider.getHosts(),
					    provider.getPort(), e.getMessage())));
	}
    }

    public void updateUser(final User user) throws DatabaseConnectionException,
	    UserDoesNotExistException {
	try {
	    BoundStatement boundStatement = selectUserStatement.bind(user
		    .getUsername());
	    ResultSet rs = provider.getSession().execute(boundStatement);
	    Row result = rs.one();
	    if (result == null) {
		throw new UserDoesNotExistException(
			new IdentifierErrorInfo(
				IdentifierErrorTemplate.USER_DOES_NOT_EXIST
					.getHttpCode(),
				IdentifierErrorTemplate.USER_DOES_NOT_EXIST
					.getErrorInfo(user.getUsername())));

	    }
	} catch (NoHostAvailableException e) {
	    throw new DatabaseConnectionException(
		    new IdentifierErrorInfo(
			    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
				    .getHttpCode(),
			    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
				    .getErrorInfo(provider.getHosts(),
					    provider.getPort(), e.getMessage())));
	}

	try {
	    BoundStatement boundStatement = updateUserStatement.bind(
		    user.getUsername(), user.getPassword());
	    provider.getSession().execute(boundStatement);
	} catch (NoHostAvailableException e) {
	    throw new DatabaseConnectionException(
		    new IdentifierErrorInfo(
			    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
				    .getHttpCode(),
			    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
				    .getErrorInfo(provider.getHosts(),
					    provider.getPort(), e.getMessage())));
	}
    }

    public void deleteUser(final String username)
	    throws DatabaseConnectionException, UserDoesNotExistException {
	try {
	    BoundStatement boundStatement = selectUserStatement.bind(username);
	    ResultSet rs = provider.getSession().execute(boundStatement);
	    Row result = rs.one();
	    if (result == null) {
		throw new UserDoesNotExistException(
			new IdentifierErrorInfo(
				IdentifierErrorTemplate.USER_DOES_NOT_EXIST
					.getHttpCode(),
				IdentifierErrorTemplate.USER_DOES_NOT_EXIST
					.getErrorInfo(username)));

	    }
	} catch (NoHostAvailableException e) {
	    throw new DatabaseConnectionException(
		    new IdentifierErrorInfo(
			    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
				    .getHttpCode(),
			    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
				    .getErrorInfo(provider.getHosts(),
					    provider.getPort(), e.getMessage())));
	}

	try {
	    BoundStatement boundStatement = deleteUserStatement.bind(username);
	    provider.getSession().execute(boundStatement);
	} catch (NoHostAvailableException e) {
	    throw new DatabaseConnectionException(
		    new IdentifierErrorInfo(
			    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
				    .getHttpCode(),
			    IdentifierErrorTemplate.DATABASE_CONNECTION_ERROR
				    .getErrorInfo(provider.getHosts(),
					    provider.getPort(), e.getMessage())));
	}
    }

    private SpringUser mapUser(final Row row) {

	final String username = row.getString("username");
	final String password = row.getString("password");
	final Set<String> roles = row.getSet("roles", String.class);
	SpringUser user = new SpringUser(username, password, roles);
	return user;
    }

    private boolean userAlreadyExistsInDB(String userName) {
	BoundStatement boundStatement = selectUserStatement.bind(userName);
	ResultSet rs = provider.getSession().execute(boundStatement);
	Row result = rs.one();
	if (result != null) {
	    return true;
	} else {
	    return false;
	}
    }
}
