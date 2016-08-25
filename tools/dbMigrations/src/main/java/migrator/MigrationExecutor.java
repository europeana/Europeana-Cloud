package migrator;

import com.contrastsecurity.cassandra.migration.CassandraMigration;
import com.contrastsecurity.cassandra.migration.config.Keyspace;

/**
 * @author krystian.
 */
public class MigrationExecutor {
    private final String[] scriptsLocations;
    private Keyspace keyspace;

    public MigrationExecutor(String cassandraKeyspace, String cassandraContactPoint, int cassandraPort, String cassandraUsername, String cassandraPassword, String[] scriptsLocations) {
        this.scriptsLocations = scriptsLocations;
        keyspace = new Keyspace();
        keyspace.setName(cassandraKeyspace);
        keyspace.getCluster().setContactpoints(cassandraContactPoint);
        keyspace.getCluster().setPort(cassandraPort);
        keyspace.getCluster().setUsername(cassandraUsername);
        keyspace.getCluster().setPassword(cassandraPassword);
    }

    public void migrate() {
        CassandraMigration cm = new CassandraMigration();
        cm.getConfigs().setScriptsLocations(scriptsLocations);
        cm.setKeyspace(keyspace);
        cm.migrate();
    }
}
