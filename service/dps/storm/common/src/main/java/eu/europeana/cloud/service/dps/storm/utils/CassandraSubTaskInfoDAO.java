package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;

import java.time.Duration;

/**
 * The {@link eu.europeana.cloud.common.model.dps.SubTaskInfo} DAO
 *
 * @author akrystian
 */
public class CassandraSubTaskInfoDAO extends CassandraDAO {

    private static final long TIME_TO_LIVE = Duration.ofDays(14).toSeconds();
    public static final int BUCKET_SIZE = 10000;

    private PreparedStatement subtaskInsertStatement;
    private PreparedStatement processedFilesCountStatement;
    private PreparedStatement removeNotificationsByTaskId;

    private static CassandraSubTaskInfoDAO instance = null;

    public static synchronized CassandraSubTaskInfoDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = new CassandraSubTaskInfoDAO(cassandra);
        }
        return instance;
    }

    /**
     * @param dbService The service exposing the connection and session
     */
    public CassandraSubTaskInfoDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    @Override
    void prepareStatements() {
        subtaskInsertStatement = dbService.getSession().prepare("INSERT INTO " + CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE + "("
                + CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID
                + "," + CassandraTablesAndColumnsNames.NOTIFICATION_BUCKET_NUMBER
                + "," + CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM
                + "," + CassandraTablesAndColumnsNames.NOTIFICATION_TOPOLOGY_NAME
                + "," + CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE
                + "," + CassandraTablesAndColumnsNames.NOTIFICATION_STATE
                + "," + CassandraTablesAndColumnsNames.NOTIFICATION_INFO_TEXT
                + "," + CassandraTablesAndColumnsNames.NOTIFICATION_ADDITIONAL_INFORMATIONS
                + "," + CassandraTablesAndColumnsNames.NOTIFICATION_RESULT_RESOURCE
                + ") VALUES (?,?,?,?,?,?,?,?,?) USING TTL "+ TIME_TO_LIVE);
        subtaskInsertStatement.setConsistencyLevel(dbService.getConsistencyLevel());


        processedFilesCountStatement = dbService.getSession().prepare(
                "SELECT resource_num FROM " + CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE +
                        " WHERE " + CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID + " = ?" +
                        " AND " + CassandraTablesAndColumnsNames.NOTIFICATION_BUCKET_NUMBER + " = ?" +
                        " ORDER BY " + CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM + " DESC limit 1");
        processedFilesCountStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        removeNotificationsByTaskId = dbService.getSession().prepare(
                "delete from " + CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE +
                        " WHERE " + CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID + " = ?" +
                        " AND " + CassandraTablesAndColumnsNames.NOTIFICATION_BUCKET_NUMBER + " = ?");

    }

    public void insert(int resourceNum, long taskId, String topologyName, String resource, String state, String infoTxt, String additionalInformations, String resultResource) {
        dbService.getSession().execute(subtaskInsertStatement.bind(taskId, bucketNumber(resourceNum), resourceNum, topologyName, resource, state, infoTxt, additionalInformations, resultResource));
    }

    public int getProcessedFilesCount(long taskId) {
        int bucketNumber = 0;
        int filesCount = 0;
        Row row;
        do {
            ResultSet rs = dbService.getSession().execute(processedFilesCountStatement.bind(taskId, bucketNumber));
            row = rs.one();
            if (row != null) {
                filesCount = row.getInt(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM);
                bucketNumber++;
            }
        } while (row != null);

        return filesCount;
    }

    public void removeNotifications(long taskId) {
        int lastBucket = bucketNumber(getProcessedFilesCount(taskId) - 1);
        for (int i = lastBucket; i >= 0; i--) {
            dbService.getSession().execute(removeNotificationsByTaskId.bind(taskId, i));
        }
    }

    public static int bucketNumber(int resourceNum) {
        return resourceNum / BUCKET_SIZE;
    }

}
