package eu.europeana.cloud.service.mcs.persistent.cassandra;

import eu.europeana.cloud.service.mcs.persistent.CassandraTestBase;
import eu.europeana.cloud.service.mcs.persistent.swift.ContentDAOTest;
import org.junit.After;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author krystian.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:/cassandraContentDAOTestContext.xml"})
public class CassandraContentDAOTest extends ContentDAOTest {

    CassandraTestBase testBase = new CassandraTestBase() {};

    @After
    public void tearDown(){
        testBase.truncateAll();
    }
}