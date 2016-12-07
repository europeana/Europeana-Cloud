package eu.europeana.cloud.test;

import org.junit.runner.Description;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

public class CassandraTestRunner extends BlockJUnit4ClassRunner {

    private final CassandraTestInstance cassandraTestInstance;
    public static final String KEYSPACE_SCHEMA_CQL = "create_cassandra_schema.cql";
    public static final String KEYSPACE = "ecloud_test";

    public CassandraTestRunner(Class c)
            throws InitializationError {
        super(c);
        cassandraTestInstance = CassandraTestInstance.getInstance(KEYSPACE_SCHEMA_CQL,KEYSPACE);
    }

    @Override
    public void run(RunNotifier rn) {
        rn.addListener(new RunListener() {
            @Override
            public void testFinished(Description description)
                    throws Exception {
                cassandraTestInstance.truncateAllData();
            }
        });
        super.run(rn);
    }
}
