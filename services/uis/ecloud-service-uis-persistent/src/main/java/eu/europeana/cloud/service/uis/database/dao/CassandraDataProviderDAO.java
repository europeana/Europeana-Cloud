package eu.europeana.cloud.service.uis.database.dao;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.service.uis.database.DatabaseService;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;

/**
 * Data provider repository using Cassandra nosql database.
 */
@Repository
public class CassandraDataProviderDAO {

    private DatabaseService dbService;

    private PreparedStatement insertNewProviderStatement;

    private PreparedStatement updateProviderStatement;

    private PreparedStatement getProviderStatement;

    private PreparedStatement deleteProviderStatement;

    private PreparedStatement getAllProvidersStatement;

    public CassandraDataProviderDAO(DatabaseService dbService){
    	this.dbService = dbService;
    	prepareStatements();
    }

    
    private void prepareStatements() {
        insertNewProviderStatement = dbService.getSession().prepare(
            "INSERT INTO data_providers(provider_id, properties, creation_date) VALUES (?,?,?) IF NOT EXISTS;");
        insertNewProviderStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        updateProviderStatement = dbService.getSession().prepare(
            "INSERT INTO data_providers(provider_id, properties, creation_date) VALUES (?,?,?);");
        updateProviderStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        getProviderStatement = dbService.getSession().prepare(
            "SELECT provider_id, properties FROM data_providers WHERE provider_id = ?;");
        getProviderStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        deleteProviderStatement = dbService.getSession().prepare(
            "DELETE FROM data_providers WHERE provider_id = ?;");
        deleteProviderStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        getAllProvidersStatement = dbService.getSession().prepare(
            "SELECT provider_id, properties FROM data_providers WHERE token(provider_id) >= token(?) LIMIT ?;");
        getAllProvidersStatement.setConsistencyLevel(dbService.getConsistencyLevel());
    }


    /**
     * Returns a sublist of providers, starting from specified provider id.
     * 
     * @param thresholdProviderId
     *            first id of provider. If null, will return beginning of the list of all providers.
     * @param limit
     *            max size of returned list.
     * @return a sublist of all providers.
     */
    public List<DataProvider> getProviders(String thresholdProviderId, int limit)
            throws NoHostAvailableException, QueryExecutionException {
        if (thresholdProviderId == null) {
            thresholdProviderId = "";
        }
        BoundStatement boundStatement = getAllProvidersStatement.bind(thresholdProviderId, limit);
        ResultSet rs = dbService.getSession().execute(boundStatement);
        List<DataProvider> dataProviders = new ArrayList<>();
        for (Row row : rs) {
            dataProviders.add(map(row));
        }
        return dataProviders;
    }


    /**
     * Returns data provider with specified id.
     * 
     * @param providerId
     *            id of provider.
     * @return data provider
     */
    public DataProvider getProvider(String providerId)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = getProviderStatement.bind(providerId);
        ResultSet rs = dbService.getSession().execute(boundStatement); //NOSONAR
        Row result = rs.one();
        if (result == null) {
            return null;
        } else {
            return map(result);
        }
    }


    /**
     * Deletes provider with specified id.
     * 
     * @param providerId
     *            id of provider.
     */
    public void deleteProvider(String providerId)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = deleteProviderStatement.bind(providerId);
        dbService.getSession().execute(boundStatement);
    }


    /**
     * Creates a new provider with specified id and properties. Will fail if provider with specified id already exists.
     * Warning: uses costly cassandra transactions.
     * 
     * @param providerId
     *            provider id
     * @param properties
     *            administrative properties of data provider
     * @deprecated This method would not work in Cassandra 2.0.1 because of bug (fixed in 2.0.2) that caused nulls
     *             inserted into rows instead of maps when using transactions.
     * @return created data provider object
     * @throws ProviderAlreadyExistsException
     *             provider with such id already exists.
     */
    @Deprecated
    public DataProvider createProvider(String providerId, DataProviderProperties properties)
            throws ProviderAlreadyExistsException, NoHostAvailableException, QueryExecutionException {
        Date now = new Date();
        BoundStatement boundStatement = insertNewProviderStatement.bind(providerId, propertiesToMap(properties), now);
        ResultSet rs = dbService.getSession().execute(boundStatement);
        boolean applied = rs.one().getBool("[applied]");
        if (!applied) {
        	throw new ProviderAlreadyExistsException(new IdentifierErrorInfo(
					IdentifierErrorTemplate.PROVIDER_ALREADY_EXISTS.getHttpCode(),
					IdentifierErrorTemplate.PROVIDER_ALREADY_EXISTS.getErrorInfo(providerId)));
        }
        DataProvider dp = new DataProvider();
        dp.setId(providerId);
        dp.setProperties(properties);
        return dp;
    }


    /**
     * Creates or updates provider with specified id.
     * 
     * @param providerId
     *            provider id
     * @param properties
     *            administrative properties of data provider
     * @return created data provider object
     */
    public DataProvider createOrUpdateProvider(String providerId, DataProviderProperties properties)
            throws NoHostAvailableException, QueryExecutionException {
        return createOrUpdateProvider(providerId, properties, new Date());
    }


    private DataProvider createOrUpdateProvider(String providerId, DataProviderProperties properties, Date date)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = updateProviderStatement.bind(providerId, propertiesToMap(properties), date);
        dbService.getSession().execute(boundStatement);
        DataProvider dp = new DataProvider();
        dp.setId(providerId);
        dp.setProperties(properties);
        return dp;
    }


    private DataProvider map(Row row) {
        DataProvider provider = new DataProvider();
        String providerId = row.getString("provider_id");

        Map<String, String> propertiesMap = row.getMap("properties", String.class, String.class);
        DataProviderProperties properties = mapToProperties(propertiesMap);
        provider.setId(providerId);
        provider.setProperties(properties);
        return provider;
    }


    //TODO: this method is not safe to changes in DataProviderProperties class. Refactor.
    private Map<String, String> propertiesToMap(DataProviderProperties properties) {
        Map<String, String> map = new HashMap<>();
        Method[] methods = DataProviderProperties.class.getDeclaredMethods();
        for (Method m : methods) {
            if (m.getName().startsWith("get")) {
                Object value;
                try {
                    value = m.invoke(properties);
                    if (value != null) {
                        map.put(m.getName().substring(3), value.toString());
                    }
                } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
                }
            }
        }
        return map;
    }


    //TODO: this method is not safe to changes in DataProviderProperties class. Refactor.
    private DataProviderProperties mapToProperties(Map<String, String> map) {
        DataProviderProperties properties = new DataProviderProperties();
        Method[] methods = DataProviderProperties.class.getDeclaredMethods();
        for (Method m : methods) {
            if (m.getName().startsWith("set")) {
                String propName = m.getName().substring(3);
                String propValue = map.get(propName);
                if (propValue != null) {
                    try {
                        m.invoke(properties, propValue);
                    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
                    }
                }
            }
        }
        return properties;
    }
}
