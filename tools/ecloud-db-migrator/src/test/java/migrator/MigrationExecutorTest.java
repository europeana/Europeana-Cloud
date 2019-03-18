package migrator;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import migrator.validators.V10_validator;
import migrator.validators.V12_validator;
import migrator.validators.V13_validator;
import migrator.validators.V2_validator;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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
        validateMigration();
        List<String> cloudIds = getCloudIds(session.execute("SELECT * FROM data_set_assignments_by_data_set;").all());
        assertThat(cloudIds.size(), is(2));
    }

    @Test
    public void shouldThrowExceptionOnTwiceDataMigrations() {
        //given
        MigrationExecutor migrator = new MigrationExecutor(EmbeddedCassandra.KEYSPACE, contactPoint, EmbeddedCassandra.PORT, cassandraUsername, cassandraPassword, scriptsLocations);
        migrator.migrate();

        //when
        migrator.migrate();

        //then
        List<String> cloudIds = getCloudIds(session.execute("SELECT * FROM data_set_assignments_by_data_set;").all());
        assertThat(cloudIds.size(), is(2));
    }

    private List<String> getCloudIds(List<Row> rows) {
        List<String> cloudIds = new ArrayList<>();
        for (Row row : rows) {
            cloudIds.add(row.getString("cloud_id"));
        }
        return cloudIds;
    }

    private void validateMigration(){
        new V10_validator(session).validate();
        new V12_validator(session).validate();
        new V13_validator(session).validate();
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

    @Test
    public void shouldSuccessfullyMigrateDataInDPS() {
        //given
        final String[] scriptsLocations1 = new String[]{"migrations/service/dps",
                "testMigrations/dps"};
        MigrationExecutor migrator = new MigrationExecutor(EmbeddedCassandra.KEYSPACE, contactPoint, EmbeddedCassandra.PORT, cassandraUsername, cassandraPassword, scriptsLocations1);
        //when
        migrator.migrate();

        //then
        validateDPSMigration();
    }

    private void validateDPSMigration() {
        new V2_validator(session).validate();
    }

}

