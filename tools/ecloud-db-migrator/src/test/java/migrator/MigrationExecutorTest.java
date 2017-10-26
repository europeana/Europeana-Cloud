package migrator;

import com.datastax.driver.core.*;
import migrator.validators.V10_validator;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author krystian.
 */
public class MigrationExecutorTest {
    private static final String contatactPoint = "localhost";
    private static final String cassandraUsername = "";
    private static final String[] scriptsLocations = new String[]{"migrations/service/mcs",
            "testMigrations/mcs"};
    private static final String cassandraPassword = "";
    private final Session session = instance.getSession();

    @ClassRule
    public static final EmbeddedCassandra instance = new EmbeddedCassandra();

    @Test
    public void shouldSuccessfullyMigrateData() {
        //given
        MigrationExecutor migrator = new MigrationExecutor(EmbeddedCassandra.KEYSPACE, contatactPoint, EmbeddedCassandra.PORT, cassandraUsername, cassandraPassword, scriptsLocations);

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
        MigrationExecutor migrator = new MigrationExecutor(EmbeddedCassandra.KEYSPACE, contatactPoint, EmbeddedCassandra.PORT, cassandraUsername, cassandraPassword, scriptsLocations);
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
    }
}