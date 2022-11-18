package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.*;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.Notification;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;

/**
 * The {@link eu.europeana.cloud.common.model.dps.SubTaskInfo} DAO
 *
 * @author akrystian
 */
@Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
public class CassandraSubTaskInfoDAO extends CassandraDAO {

    private static CassandraSubTaskInfoDAO instance = null;
    public static final int BUCKET_SIZE = 10000;

    private PreparedStatement subtaskInsertStatement;
    private PreparedStatement processedFilesCountStatement;
    private PreparedStatement removeNotificationsByTaskId;

    /**
     * @param dbService The service exposing the connection and session
     */
    public CassandraSubTaskInfoDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    public CassandraSubTaskInfoDAO() {
        //needed for creating cglib proxy in RetryableMethodExecutor.createRetryProxy()
    }

    public static synchronized CassandraSubTaskInfoDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = RetryableMethodExecutor.createRetryProxy(new CassandraSubTaskInfoDAO(cassandra));
        }
        return instance;
    }


    @Override
    protected void prepareStatements() {
        subtaskInsertStatement = dbService.getSession().prepare(
                "INSERT INTO " + CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE
                        + "("
                        + CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID
                        + "," + CassandraTablesAndColumnsNames.NOTIFICATION_BUCKET_NUMBER
                        + "," + CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM
                        + "," + CassandraTablesAndColumnsNames.NOTIFICATION_TOPOLOGY_NAME
                        + "," + CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE
                        + "," + CassandraTablesAndColumnsNames.NOTIFICATION_STATE
                        + "," + CassandraTablesAndColumnsNames.NOTIFICATION_INFO_TEXT
                        + "," + CassandraTablesAndColumnsNames.NOTIFICATION_ADDITIONAL_INFORMATION
                        + "," + CassandraTablesAndColumnsNames.NOTIFICATION_RESULT_RESOURCE
                        + ") VALUES (?,?,?,?,?,?,?,?,?)"
        );

        processedFilesCountStatement = dbService.getSession().prepare(
                "SELECT resource_num "
                        + "FROM " + CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE
                        + " WHERE " + CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID + " = ?"
                        + " AND " + CassandraTablesAndColumnsNames.NOTIFICATION_BUCKET_NUMBER + " = ?"
                        + " ORDER BY " + CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM + " DESC limit 1"
        );

        removeNotificationsByTaskId = dbService.getSession().prepare(
                "DELETE FROM " + CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE
                        + " WHERE " + CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID + " = ?"
                        + " AND " + CassandraTablesAndColumnsNames.NOTIFICATION_BUCKET_NUMBER + " = ?"
        );

    }

    public void insert(int resourceNum, long taskId, String topologyName, String resource, String state,
                       String infoTxt, String additionalInformation, String resultResource) {
        dbService.getSession().execute(
                subtaskInsertStatement.bind(taskId, bucketNumber(resourceNum), resourceNum, topologyName,
                        resource, state, infoTxt, additionalInformation, resultResource)
        );
    }

    public BoundStatement insertNotificationStatement(Notification notification) {
        return subtaskInsertStatement.bind(
                notification.getTaskId(),
                bucketNumber(notification.getResourceNum()),
                notification.getResourceNum(),
                notification.getTopologyName(),
                notification.getResource(),
                notification.getState(),
                notification.getInfoText(),
                notification.getAdditionalInformation(),
                notification.getResultResource());
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
