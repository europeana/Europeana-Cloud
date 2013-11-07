package eu.europeana.cloud.service.mcs.persistent;

import org.cassandraunit.spring.CassandraDataSet;
import org.cassandraunit.spring.CassandraUnitTestExecutionListener;
import org.cassandraunit.spring.EmbeddedCassandra;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/default-context.xml"})
@TestExecutionListeners({CassandraUnitTestExecutionListener.class})
@CassandraDataSet(keyspace = CassandraTest.KEYSPACE, value = {"create_cassandra_schema.cql"})
@EmbeddedCassandra(host = CassandraTest.HOST, port = CassandraTest.PORT)
public class CassandraTest {

    public static final String HOST = "127.0.0.1";

    public static final int PORT = 9142;

    public static final String KEYSPACE = "ecloud_test";


    @Test
    public void should_work() {
        test();
    }


    @Test
    public void should_work_twice() {
        test();
    }


    private void test() {
        Cluster cluster = Cluster.builder()
                .addContactPoints(HOST)
                .withPort(PORT)
                .build();
        Session session = cluster.connect(KEYSPACE);
        ResultSet result = session.execute("select * from representations ;");
    }
}
