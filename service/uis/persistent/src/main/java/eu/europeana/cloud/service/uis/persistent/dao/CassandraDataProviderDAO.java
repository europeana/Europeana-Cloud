package eu.europeana.cloud.service.uis.persistent.dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Data provider repository using Cassandra nosql database.
 */
@Repository
public class CassandraDataProviderDAO {

    private CassandraConnectionProvider dbService;

    private PreparedStatement createDataProviderStatement;
    
    private PreparedStatement updateDataProviderStatement;

    private PreparedStatement getProviderStatement;

    private PreparedStatement deleteProviderStatement;

    private PreparedStatement getAllProvidersStatement;
    
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraDataProviderDAO.class);

    /**
     * 
     * Creates a new instance of this class.
     * @param dbService Connector to Cassandra cluster
     */
    public CassandraDataProviderDAO(CassandraConnectionProvider dbService){
    	this.dbService = dbService;
    	prepareStatements();
    }

    
    private void prepareStatements() {
        createDataProviderStatement = dbService.getSession().prepare(
            "INSERT INTO data_providers(provider_id, active, properties, creation_date, partition_key) VALUES (?,true,?,?,?);");
        createDataProviderStatement.setConsistencyLevel(dbService.getConsistencyLevel());
        
        updateDataProviderStatement = dbService.getSession().prepare(
            "UPDATE data_providers SET active=?, properties=? where provider_id = ?;");
        updateDataProviderStatement.setConsistencyLevel(dbService.getConsistencyLevel());
        
        getProviderStatement = dbService.getSession().prepare(
            "SELECT provider_id, partition_key, active, properties FROM data_providers WHERE provider_id = ?;");
        getProviderStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        deleteProviderStatement = dbService.getSession().prepare(
            "DELETE FROM data_providers WHERE provider_id = ?;");
        deleteProviderStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        getAllProvidersStatement = dbService.getSession().prepare(
            "SELECT provider_id, active, partition_key, properties FROM data_providers WHERE token(provider_id) >= token(?) LIMIT ?;");
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
     * @throws NoHostAvailableException 
     * @throws QueryExecutionException 
     */
    public List<DataProvider> getProviders(String thresholdProviderId, int limit)
            throws NoHostAvailableException, QueryExecutionException {
    	String provId = thresholdProviderId;
        if (provId == null) {
            provId = "";
        }
        BoundStatement boundStatement = getAllProvidersStatement.bind(provId, limit);
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
     * @throws NoHostAvailableException 
     * @throws QueryExecutionException 
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
     * @throws NoHostAvailableException 
     * @throws QueryExecutionException 
     */
    public void deleteProvider(String providerId)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = deleteProviderStatement.bind(providerId);
        dbService.getSession().execute(boundStatement);
    }


    /**
     * 
     * Creates new data-provider with specified id and properties. Newly created provider is 'active' by default.
     * 
     * @param providerId provider id
     * @param properties administrative properties of data provider
     * @return created data provider object
     * @throws NoHostAvailableException
     * @throws QueryExecutionException
     */
    public DataProvider createDataProvider(String providerId, DataProviderProperties properties)
            throws NoHostAvailableException, QueryExecutionException {
        int partitionKey = providerId.hashCode();
        BoundStatement boundStatement = createDataProviderStatement.bind(providerId, propertiesToMap(properties), new Date(), partitionKey);
        dbService.getSession().execute(boundStatement);
        DataProvider dp = new DataProvider();
        dp.setId(providerId);
        dp.setPartitionKey(partitionKey);
        dp.setProperties(properties);
        return dp;
    }

    /**
     * 
     * Updates data provider in DB (properties and 'active' flag)
     * 
     * @param dataProvider data provider object
     * @return updated data provider
     * @throws NoHostAvailableException
     * @throws QueryExecutionException
     */
    public DataProvider updateDataProvider(DataProvider dataProvider) throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = updateDataProviderStatement.bind(dataProvider.isActive(), propertiesToMap(dataProvider.getProperties()),dataProvider.getId());
        dbService.getSession().execute(boundStatement);
        return dataProvider;
    }


    private DataProvider map(Row row) {
        DataProvider provider = new DataProvider();
        String providerId = row.getString("provider_id");
        int partitionKey = row.getInt("partition_key");

        Map<String, String> propertiesMap = row.getMap("properties", String.class, String.class);
        DataProviderProperties properties = mapToProperties(propertiesMap);
        provider.setId(providerId);
        provider.setPartitionKey(partitionKey);
        provider.setProperties(properties);
        provider.setActive(row.getBool("active"));
        return provider;
    }


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
                	LOGGER.error(ex.getMessage());
                }
            }
        }
        return map;
    }


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
                    	LOGGER.error(ex.getMessage());
                    }
                }
            }
        }
        return properties;
    }
}
