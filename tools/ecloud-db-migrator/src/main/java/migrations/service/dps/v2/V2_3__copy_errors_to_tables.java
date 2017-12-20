package migrations.service.dps.v2;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.datastax.driver.core.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class V2_3__copy_errors_to_tables implements JavaMigration {

    private static final String SELECT_NOTIFICATION_STATEMENT = "SELECT * FROM notifications;";

    private static final String UPDATE_BASIC_INFO_STATEMENT = "UPDATE basic_info set errors = ? where task_id = ?;";

    private static final String INSERT_ERROR_NOTIFICATIONS_STATEMENT = "INSERT INTO error_notifications" +
            " (task_id,error_type,error_message,resource) VALUES (?,?,?,?);";

    private static final String UPDATE_ERROR_COUNTERS_STATEMENT = "UPDATE error_counters" +
            " set error_count = error_count + 1 WHERE task_id = ? AND error_type = ?;";

    private static final String SUCCESS = "SUCCESS";

    private static final String STATE = "state";

    private static final String INFO = "info_text";

    private static final String TASK_ID = "task_id";

    private static final String RESOURCE = "resource";

    private PreparedStatement selectNotification;

    private PreparedStatement updateErrors;

    private PreparedStatement insertErrorNotifications;

    private PreparedStatement updateErrorCounters;

    @Override
    public void migrate(Session session) {
        initStatements(session);

        Map<Long, Map<String, Integer>> errorsPerTask = new HashMap<>();
        Map<String, String> errorTypes = new HashMap<>();
        Map<Long, Integer> errorCounts = new HashMap<>();

        BoundStatement boundStatement = selectNotification.bind();
        boundStatement.setFetchSize(100);
        ResultSet rs = session.execute(boundStatement);
        for (Row notificationRow : rs) {
            if (!SUCCESS.equals(notificationRow.getString(STATE))) {
                Long taskId = notificationRow.getLong(TASK_ID);
                String errorMessage = notificationRow.getString(INFO);
                if (errorMessage != null && !errorMessage.isEmpty()) {
                    String errorType = getErrorType(errorTypes, errorMessage);
                    updateTables(session, errorType, errorMessage, taskId, notificationRow.getString(RESOURCE));
                    increaseCounts(errorsPerTask, errorCounts, taskId, errorType);
                }
            }
        }
        updateErrorsForTasks(session, errorCounts);
    }

    private void updateErrorsForTasks(Session session, Map<Long, Integer> errorCounts) {
        BoundStatement bs;
        for (Map.Entry<Long, Integer> entry : errorCounts.entrySet()) {
            bs = updateErrors.bind(entry.getValue(), entry.getKey());
            session.execute(bs);
        }
    }

    private void increaseCounts(Map<Long, Map<String, Integer>> errorsPerTask, Map<Long, Integer> errorCounts, Long taskId, String errorType) {
        Map<String, Integer> counts = getErrorCounts(errorsPerTask, taskId);
        Integer count = counts.get(errorType);
        if (count == null) {
            count = 1;
        } else {
            count = count + 1;
        }
        counts.put(errorType, count);

        Integer allCount = errorCounts.get(taskId);
        if (allCount == null) {
            errorCounts.put(taskId, count);
        } else {
            errorCounts.put(taskId, allCount + 1);
        }
    }

    private Map<String, Integer> getErrorCounts(Map<Long, Map<String, Integer>> errorsPerTask, Long taskId) {
        Map<String, Integer> counts = errorsPerTask.get(taskId);
        if (counts == null) {
            counts = new HashMap<>();
            errorsPerTask.put(taskId, counts);
        }
        return counts;
    }

    private String getErrorType(Map<String, String> errorTypes, String errorMessage) {
        String errorType = errorTypes.get(errorMessage);
        if (errorType == null) {
            errorType = new com.eaio.uuid.UUID().toString();
            errorTypes.put(errorMessage, errorType);
        }
        return errorType;
    }

    private void updateTables(Session session, String errorType, String errorMessage, long taskId, String resource) {
        BoundStatement bs = insertErrorNotifications.bind(taskId, UUID.fromString(errorType), errorMessage, resource);
        session.execute(bs);

        bs = updateErrorCounters.bind(taskId, UUID.fromString(errorType));
        session.execute(bs);
    }

    private void initStatements(Session session) {
        selectNotification = session.prepare(SELECT_NOTIFICATION_STATEMENT);
        selectNotification.setConsistencyLevel(ConsistencyLevel.QUORUM);

        updateErrors = session.prepare(UPDATE_BASIC_INFO_STATEMENT);
        updateErrors.setConsistencyLevel(ConsistencyLevel.QUORUM);

        insertErrorNotifications = session.prepare(INSERT_ERROR_NOTIFICATIONS_STATEMENT);
        insertErrorNotifications.setConsistencyLevel(ConsistencyLevel.QUORUM);

        updateErrorCounters = session.prepare(UPDATE_ERROR_COUNTERS_STATEMENT);
        updateErrorCounters.setConsistencyLevel(ConsistencyLevel.QUORUM);
    }
}
