import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.contrastsecurity.cassandra.migration.logging.console.ConsoleLog;
import com.contrastsecurity.cassandra.migration.logging.console.ConsoleLogCreator;
import com.datastax.driver.core.*;
import migrator.MigrationExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author krystian.
 */
public class MigrationExecutorTest extends CassandraBaseTest{


    private String contatactPoint;
    private String cassandraUsername;
    private String[] scriptsLocations;
    private String cassandraPassword;
    private Session session;

    @Before
    public void setUp() throws Exception {
        createKeyspaces();
        LogFactory.setLogCreator(new ConsoleLogCreator(ConsoleLog.Level.DEBUG));

        contatactPoint = "localhost";
        cassandraUsername = "";
        cassandraPassword = "";
        scriptsLocations = new String[] {"migrations/service/mcs",
                "testMigrations/mcs"};

        Cluster cluster = Cluster.builder().addContactPoints(contatactPoint).withPort(PORT)
                .withProtocolVersion(ProtocolVersion.V3)
                .withTimestampGenerator(new AtomicMonotonicTimestampGenerator())
                .withCredentials(cassandraUsername, cassandraPassword).build();
        session = cluster.connect(KEYSPACE);

    }

    @After
    public void tearDown(){
        dropAllKeyspaces();
    }

    @Test
    public void shouldSuccessfullyMigrateData() {
        //given
        MigrationExecutor migrator = new MigrationExecutor(KEYSPACE, contatactPoint, PORT, cassandraUsername, cassandraPassword, scriptsLocations);
        //when
        migrator.migrate();
        //then
        List<String> cloudIds = getCloudIds(session.execute("SELECT * FROM data_set_assignments;").all());
        assertThat(cloudIds.size(), is(2));
    }

    private List<String> getCloudIds(List<Row> rows) {
        List<String> cloudIds = new ArrayList<>();
        for (Row row : rows) {
            cloudIds.add(row.getString("cloud_id"));
        }
        return cloudIds;
    }

    @Test
    public void shouldThrowExceptionOnTwiceDataMigrations() {
        //given
        MigrationExecutor migrator = new MigrationExecutor(KEYSPACE, contatactPoint, PORT, cassandraUsername, cassandraPassword, scriptsLocations);
        migrator.migrate();
        //when
        migrator.migrate();

        //then
        List<String> cloudIds = getCloudIds(session.execute("SELECT * FROM data_set_assignments;").all());
        assertThat(cloudIds.size(), is(2));
    }



}