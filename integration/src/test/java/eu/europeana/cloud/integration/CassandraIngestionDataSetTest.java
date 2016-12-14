/* IngestionDataSetTest.java - created on Jan 7, 2014, Copyright (c) 2011 The European Library, all rights reserved */
package eu.europeana.cloud.integration;

import com.datastax.driver.core.Session;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * This class test integration concerning operation on dataset basis. It tests the whole workflow
 * concerning ingestion of whole datasets as well as reading them.
 * 
 * @author Markus Muhr (markus.muhr@kb.nl)
 * @since Jan 7, 2014
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/persistentServicesTestContext.xml" })
public class CassandraIngestionDataSetTest extends IngestionDataSetTest {
    // config:
    private static final String KEYSPACE              = "integration_test";
    private static final String KEYSPACE_SCHEMA_CQL   = "cassandra.cql";

    /**
     * Creates a new instance of this class.
     */
    public CassandraIngestionDataSetTest() {
        CassandraTestInstance.getInstance(KEYSPACE_SCHEMA_CQL,KEYSPACE);
    }

    /**
     * @return session
     */
    protected Session getSession() {

        return CassandraTestInstance.getSession(KEYSPACE);
    }

    /**
     * Truncates all tables.
     */
    @Before
    public void truncateAll() {
        CassandraTestInstance.truncateAllData(false);
    }
}
