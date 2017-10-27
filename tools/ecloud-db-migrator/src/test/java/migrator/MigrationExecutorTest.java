package migrator;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author krystian.
 */
public class MigrationExecutorTest {
    private static final String contactPoint = "localhost";
    private static final String cassandraUsername = "";
    private static final String[] scriptsLocations = new String[]{"migrations/service/mcs",
            "testMigrations/mcs"};
    private static final String cassandraPassword = "";
    private final Session session = instance.getSession();

    @ClassRule
    public static final EmbeddedCassandra instance = new EmbeddedCassandra();


    @Before
    public void deleteMigrationEntries() {
        session.execute("DROP TABLE IF EXISTS cassandra_migration_version");
        session.execute("DROP TABLE IF EXISTS cassandra_migration_version_counts");

    }


    @Test
    public void shouldSuccessfullyMigrateData() {
        //given
        MigrationExecutor migrator = new MigrationExecutor(EmbeddedCassandra.KEYSPACE, contactPoint, EmbeddedCassandra.PORT, cassandraUsername, cassandraPassword, scriptsLocations);

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
    public void shouldSuccessfullyMigrateDataInUIS() {
        //given
        final String[] scriptsLocations1 = new String[]{"migrations/service/uis",
                "testMigrations/uis"};
        MigrationExecutor migrator = new MigrationExecutor(EmbeddedCassandra.KEYSPACE, contactPoint, EmbeddedCassandra.PORT, cassandraUsername, cassandraPassword, scriptsLocations1);
        //when
        migrator.migrate();
        List<Row> rows = session.execute("SELECT * FROM provider_record_id;").all();
        assertEquals(rows.size(), 1);
        Row row = rows.get(0);
        assertTheMigratedTableValues(row);
    }

    private void assertTheMigratedTableValues(Row row) {
        assertNotNull(row.getString("provider_id"));
        assertEquals(row.getString("provider_id"), "provider_id");
        assertNotNull(row.getUUID("bucket_id").toString());
        assertEquals(row.getString("record_id"), "record_id");
        assertEquals(row.getString("cloud_id"), "cloud_id");
    }


}

