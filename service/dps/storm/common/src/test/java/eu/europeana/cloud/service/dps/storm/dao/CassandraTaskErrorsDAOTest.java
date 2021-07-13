package eu.europeana.cloud.service.dps.storm.dao;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class CassandraTaskErrorsDAOTest extends CassandraTestBase {

    public static final String ERROR_TYPE_1 = "03e473e0-a201-11e7-a8ab-0242ac110009";
    public static final String ERROR_TYPE_2 = "03e473e0-a201-11e7-a8ab-0242ac110010";
    private CassandraTaskErrorsDAO cassandraTaskErrorsDAO;

    @Before
    public void setup() {
        CassandraConnectionProvider db = new CassandraConnectionProvider(HOST, CassandraTestInstance.getPort(), KEYSPACE, USER, PASSWORD);
        cassandraTaskErrorsDAO = CassandraTaskErrorsDAO.getInstance(db);
    }

    @Test
    public void shouldReturnZeroErrorsForTaskThatDoesNotExist() {
        long errorCount = cassandraTaskErrorsDAO.selectErrorCountsForErrorType(1, UUID.fromString(ERROR_TYPE_1));
        assertEquals(0, errorCount);
    }

    @Test
    public void shouldReturnCorrectNumberOfErrorsForTask() {
        cassandraTaskErrorsDAO.updateErrorCounter(1, ERROR_TYPE_1);
        cassandraTaskErrorsDAO.updateErrorCounter(1, ERROR_TYPE_1);
        cassandraTaskErrorsDAO.updateErrorCounter(1, ERROR_TYPE_1);
        cassandraTaskErrorsDAO.updateErrorCounter(1, ERROR_TYPE_1);
        cassandraTaskErrorsDAO.updateErrorCounter(1, ERROR_TYPE_2);

        long errorCount = cassandraTaskErrorsDAO.selectErrorCountsForErrorType(1, UUID.fromString(ERROR_TYPE_1));
        assertEquals(4, errorCount);

        errorCount = cassandraTaskErrorsDAO.selectErrorCountsForErrorType(1, UUID.fromString(ERROR_TYPE_2));
        assertEquals(1, errorCount);
    }


}