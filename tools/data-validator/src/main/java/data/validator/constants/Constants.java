package data.validator.constants;

/**
 * Created by Tarek on 4/27/2017.
 */
public final class Constants {
    public static final String CLUSTERING_KEY_TYPE = "clustering_key";
    public static final String PARTITION_KEY_TYPE = "partition_key";
    public static final String COLUMN_NAME_SELECTOR = "column_name";
    public static final String SYSTEM_SCHEMA_COLUMNS_TABLE = "system.schema_columns";
    public static final String COLUMN_INDEX_TYPE = "type";
    public static final String KEYSPACE_NAME_LABEL = "keyspace_name";
    public static final String TABLE_NAME_LABEL = "columnfamily_name";
    public static final int PROGRESS_COUNTER = 10000;

    public static final String SOURCE_HOSTS = "sourceHosts";
    public static final String SOURCE_PORT = "sourcePort";
    public static final String SOURCE_KEYSPACE = "sourceKeyspace";
    public static final String SOURCE_USER_NAME = "sourceUserName";
    public static final String SOURCE_PASSWORD = "sourcePassword";

    public static final String TARGET_HOSTS = "targetHosts";
    public static final String TARGET_PORT = "targetPort";
    public static final String TARGET_KEYSPACE = "targetKeyspace";
    public static final String TARGET_USER_NAME = "targetUserName";
    public static final String TARGET_PASSWORD = "targetPassword";

    public static final String SOURCE_TABLE = "sourceTable";
    public static final String TARGET_TABLE = "targetTable";
    public static final String THREADS_COUNT = "threads";
    public static final String CONFIGURATION_PROPERTIES = "configuration";
    public static final int DEFAULT_THREADS_COUNT = 10;
    public static final String DEFAULT_PROPERTIES_FILE = "test.properties";

}
