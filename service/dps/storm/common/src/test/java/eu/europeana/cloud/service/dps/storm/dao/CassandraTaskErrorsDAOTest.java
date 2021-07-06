package eu.europeana.cloud.service.dps.storm.dao;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class CassandraTaskErrorsDAOTest extends CassandraTestBase {

    private CassandraTaskErrorsDAO cassandraTaskErrorsDAO;

    @Before
    public void setup() {
        CassandraConnectionProvider db = new CassandraConnectionProvider(HOST, CassandraTestInstance.getPort(), KEYSPACE, USER, PASSWORD);
        cassandraTaskErrorsDAO = CassandraTaskErrorsDAO.getInstance(db);
    }

    @Test
    public void shouldReturnZeroErrorsForTaskThatDoesNotExist() {
        long errorCount = cassandraTaskErrorsDAO.getErrorCount(1, UUID.fromString("03e473e0-a201-11e7-a8ab-0242ac110009"));
        assertEquals(0, errorCount);
    }

    @Test
    public void shouldReturnCorrectNumberOfErrorsForTask() {
        cassandraTaskErrorsDAO.updateErrorCounter(1, "03e473e0-a201-11e7-a8ab-0242ac110009");
        cassandraTaskErrorsDAO.updateErrorCounter(1, "03e473e0-a201-11e7-a8ab-0242ac110009");
        cassandraTaskErrorsDAO.updateErrorCounter(1, "03e473e0-a201-11e7-a8ab-0242ac110009");
        cassandraTaskErrorsDAO.updateErrorCounter(1, "03e473e0-a201-11e7-a8ab-0242ac110009");
        cassandraTaskErrorsDAO.updateErrorCounter(1, "03e473e0-a201-11e7-a8ab-0242ac110010");

        long errorCount = cassandraTaskErrorsDAO.getErrorCount(1, UUID.fromString("03e473e0-a201-11e7-a8ab-0242ac110009"));
        assertEquals(4, errorCount);

        errorCount = cassandraTaskErrorsDAO.getErrorCount(1, UUID.fromString("03e473e0-a201-11e7-a8ab-0242ac110010"));
        assertEquals(1, errorCount);
    }


}