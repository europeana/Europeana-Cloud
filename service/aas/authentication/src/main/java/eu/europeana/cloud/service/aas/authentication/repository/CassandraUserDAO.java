package eu.europeana.cloud.service.aas.authentication.repository;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.collect.ImmutableSet;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.model.User;
import eu.europeana.cloud.service.aas.authentication.SpringUser;
import eu.europeana.cloud.service.aas.authentication.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.aas.authentication.status.IdentifierErrorTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Set;

/**
 * Used to create / retrieve / update <code>User</code> objects from ecloud's
 * database.
 *
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 */
@Retryable
public class CassandraUserDAO {

    private static final Log LOGGER = LogFactory.getLog(CassandraUserDAO.class);
    /**
     * Default roles for all users of ecloud (that don't have admin, or other
     * superpowers)
     */
    private static Set<String> DEFAULT_USER_ROLES = ImmutableSet
            .of("ROLE_USER");
    @Qualifier("dbService")
    private CassandraConnectionProvider provider;
    private PreparedStatement selectUserStatement;
    private PreparedStatement createUserStatement;
    private PreparedStatement updateUserStatement;
    private PreparedStatement deleteUserStatement;

    /**
     * Constructs a new <code>CassandraUserDAO</code>.
     *
     * @param provider the <code>Session</code> to use for connectivity with
     *                 Cassandra.
     */
    public CassandraUserDAO(final CassandraConnectionProvider provider) {
        LOGGER.info("CassandraUserDAO starting...");
        this.provider = provider;
        prepareStatements();
        LOGGER.info("CassandraUserDAO started successfully.");
    }

    public SpringUser getUser(final String username)
            throws DatabaseConnectionException {
        try {
            BoundStatement boundStatement = selectUserStatement.bind(username);
            ResultSet rs = provider.getSession().execute(boundStatement);
            Row result = rs.one();
            if (result == null) {
                return null;
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

    public void createUser(final User user) throws DatabaseConnectionException {

        try {
            BoundStatement boundStatement = createUserStatement.bind(
                    user.getUsername(), user.getPassword(), DEFAULT_USER_ROLES);
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

    public void updateUser(final User user) throws DatabaseConnectionException {

        try {
            BoundStatement boundStatement = updateUserStatement.bind(
                    user.getPassword(), user.getUsername());
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
            throws DatabaseConnectionException {

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

    private SpringUser mapUser(final Row row) {

        final String username = row.getString("username");
        final String password = row.getString("password");
        final Set<String> roles = row.getSet("roles", String.class);
        SpringUser user = new SpringUser(username, password, roles);
        return user;
    }
}
