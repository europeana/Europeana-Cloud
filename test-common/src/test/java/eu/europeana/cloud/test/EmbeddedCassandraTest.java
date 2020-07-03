package eu.europeana.cloud.test;

import com.datastax.driver.core.Row;

import eu.europeana.cloud.test.CassandraConnectionProvider;
import eu.europeana.cloud.test.CassandraTestRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;


import static eu.europeana.cloud.test.CassandraTestRunner.EMBEEDED_CASSANDRA_PORT;
import static eu.europeana.cloud.test.CassandraTestRunner.JUNIT_AAS_KEYSPACE;
import static org.junit.Assert.assertEquals;


/**
 * DataSetResourceTest
 */
@RunWith(CassandraTestRunner.class)
public class EmbeddedCassandraTest {

    @Before
    public void mockUp() {
    }

    @Test
    public void checkConnection()
            throws Exception {
        System.out.println("Wstaje cassandra!");
        System.out.println(aasCassandraProvider().getSession().execute("INSERT INTO users (username, password, roles) VALUES('admin', 'ecloud_admin', {'ROLE_ADMIN'});").all().size());
        System.out.println(aasCassandraProvider().getSession().execute("INSERT INTO users (username, password, roles) VALUES('user', 'ecloud_user', {'ROLE_USER'});").all().size());
        List<Row> users = aasCassandraProvider().getSession().execute("select * from users").all();
        System.out.println(users.size());
        for (Row user : users) {
            System.out.println(""+user);
        }
        assertEquals(2, users.size());
    }

    public CassandraConnectionProvider aasCassandraProvider() {
        return new CassandraConnectionProvider("localhost",
                EMBEEDED_CASSANDRA_PORT
                , JUNIT_AAS_KEYSPACE, "", "");
    }

}
