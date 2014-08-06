package eu.europeana.cloud.service.aas.authentication.repository;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.service.aas.authentication.CloudUserDetails;
import org.springframework.security.core.userdetails.UserDetails;

/**
 * Used to create / retrieve / update <code>User</code> objects from ecloud's
 * database.
 *
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 *
 */
public class CassandraUserDAO {

    private static final Log LOGGER = LogFactory.getLog(CassandraUserDAO.class);

    private Session session;

    private ConsistencyLevel cassandraConsistencyLevel;

    private PreparedStatement createUserStatement;

    private PreparedStatement updateUserStatement;

    private PreparedStatement selectUserStatement;

    /**
     * Constructs a new <code>CassandraUserDAO</code>.
     *
     * @param session the <code>Session</code> to use for connectivity with
     * Cassandra.
     */
    public CassandraUserDAO(final CassandraConnectionProvider databaseService) {
        this(databaseService.getSession(), databaseService.getConsistencyLevel());
    }

    /**
     * Constructs a new <code>CassandraUserDAO</code>.
     *
     * Used to create / retrieve / update <code>User</code> objects from
     * ecloud's database.
     *
     * @param session session the <code>Session</code> to use for connectivity
     * with Cassandra.
     * @param cassandraConsistencyLevel Consistency Level for Cassandra.
     */
    public CassandraUserDAO(final Session session, final ConsistencyLevel cassandraConsistencyLevel) {

        LOGGER.info("CassandraUserDAO starting...");

        this.session = session;
        this.cassandraConsistencyLevel = cassandraConsistencyLevel;
        prepareStatements();

        LOGGER.info("CassandraUserDAO started successfully.");
    }

    public UserDetails getUser(final String username) {
        return retrieveUser(username);
    }

    private void prepareStatements() {
        createUserStatement = session.prepare(
                "INSERT INTO data_providers(provider_id, properties, creation_date) VALUES (?,?,?) IF NOT EXISTS;");
        createUserStatement.setConsistencyLevel(cassandraConsistencyLevel);

        updateUserStatement = session.prepare(
                "INSERT INTO data_providers(provider_id, properties, creation_date) VALUES (?,?,?);");
        updateUserStatement.setConsistencyLevel(cassandraConsistencyLevel);

        selectUserStatement = session.prepare(
                "SELECT username, password FROM USERS WHERE username = ?");
        selectUserStatement.setConsistencyLevel(cassandraConsistencyLevel);
    }

    private UserDetails retrieveUser(final String userName)
            throws NoHostAvailableException, QueryExecutionException {

        BoundStatement boundStatement = selectUserStatement.bind(userName);
        ResultSet rs = session.execute(boundStatement);
        Row result = rs.one();
        if (result == null) {
            return null;
        }
        return mapUser(result);
    }

    private void createUser(final String userName, final String password)
            throws NoHostAvailableException, QueryExecutionException {

//        BoundStatement boundStatement = createUserStatement.bind(userName, propertiesToMap(properties), date);
//        session.execute(boundStatement);
    }

    private UserDetails mapUser(final Row row) {
        final String username = row.getString("username");
        final String password = row.getString("password");

        UserDetails user = new CloudUserDetails(username, password);
        return user;
    }

    /**
     * private Map<String, String> propertiesToMap(DataProviderProperties
     * properties) { Map<String, String> map = new HashMap<>(); Method[] methods
     * = DataProviderProperties.class.getDeclaredMethods(); for (Method m :
     * methods) { if (m.getName().startsWith("get")) { Object value; try { value
     * = m.invoke(properties); if (value != null) {
     * map.put(m.getName().substring(3), value.toString()); } } catch
     * (IllegalAccessException | IllegalArgumentException |
     * InvocationTargetException ex) { LOGGER.error(ex.getMessage()); } } }
     * return map; }
     *
     *
     * private DataProviderProperties mapToProperties(Map<String, String> map) {
     * DataProviderProperties properties = new DataProviderProperties();
     * Method[] methods = DataProviderProperties.class.getDeclaredMethods(); for
     * (Method m : methods) { if (m.getName().startsWith("set")) { String
     * propName = m.getName().substring(3); String propValue =
     * map.get(propName); if (propValue != null) { try { m.invoke(properties,
     * propValue); } catch (IllegalAccessException | IllegalArgumentException |
     * InvocationTargetException ex) { LOGGER.error(ex.getMessage()); } } } }
     * return properties; }
     */
    /**
     * Retrieve the consistency level of the queries
     *
     * @return QUORUM consistency level
     */
    public ConsistencyLevel getConsistencyLevel() {
        return cassandraConsistencyLevel;
    }

}
