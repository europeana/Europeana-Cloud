package eu.europeana.cloud.service.dps.service.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Report service powered by Cassandra.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class CassandraReportService implements TaskExecutionReportService {
    CassandraConnectionProvider cassandra;

    /**
     * Constructor of Cassandra report service.
     *
     * @param hosts        Cassandra hosts separated by comma (e.g. localhost,192.168.47.129)
     * @param port         Cassandra port
     * @param keyspaceName Cassandra keyspace name
     * @param userName     Cassandra username
     * @param password     Cassandra password
     */
    public CassandraReportService(String hosts, int port, String keyspaceName, String userName, String password) {
        cassandra = new CassandraConnectionProvider(hosts, port, keyspaceName, userName, password);
    }

    @Override
    public String getTaskProgress(String taskId) throws AccessDeniedOrObjectDoesNotExistException {
        long taskId_ = Long.valueOf(taskId);

        JsonObject res = new JsonObject();
        res.addProperty("taskId", taskId_);

        //read basic informations from Cassandra
        Statement selectFromBasicInfo = QueryBuilder.select().all()
                .from(CassandraTablesAndColumnsNames.BASIC_INFO_TABLE)
                .where(QueryBuilder.eq(CassandraTablesAndColumnsNames.BASIC_TASK_ID, taskId_));

        Row basicInfo = cassandra.getSession().execute(selectFromBasicInfo).one();
        long processed = 0;
        //basicInfo == null means: task has been dropped or task is still running and calcutalion of expected size is in progress
        if (basicInfo != null) {
            int expectedSize = basicInfo.getInt(CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE);
            if (expectedSize > 0) {

                //read number of processed tasks from Cassandra
                Statement selectFromNotification = QueryBuilder.select().countAll()
                        .from(CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE)
                        .where(QueryBuilder.eq(CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID, taskId_))
                        .limit(expectedSize);

                ResultSet notifications = cassandra.getSession().execute(selectFromNotification);

                processed = notifications.one().getLong("count");
            }
            res.addProperty("topologyName", basicInfo.getString(CassandraTablesAndColumnsNames.BASIC_TOPOLOGY_NAME));
            res.addProperty("totalSize", expectedSize);
            res.addProperty("processed", processed);
            res.addProperty("state", basicInfo.getString(CassandraTablesAndColumnsNames.STATE));
            res.addProperty("info", basicInfo.getString(CassandraTablesAndColumnsNames.INFO));
            res.addProperty("sent_time", prepareDate(basicInfo.getDate(CassandraTablesAndColumnsNames.SENT_TIME)));
            res.addProperty("start_time", prepareDate(basicInfo.getDate(CassandraTablesAndColumnsNames.START_TIME)));
            res.addProperty("finish_time", prepareDate(basicInfo.getDate(CassandraTablesAndColumnsNames.FINISH_TIME)));
        } else {
            //read number of processed tasks from Cassandra
            Statement selectFromNotification = QueryBuilder.select().countAll()
                    .from(CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE)
                    .where(QueryBuilder.eq(CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID, taskId_));

            ResultSet notifications = cassandra.getSession().execute(selectFromNotification);

            processed = notifications.one().getLong("count");

            res.addProperty("topologyName", "");
            res.addProperty("totalSize", "?");
            res.addProperty("processed", processed);
            res.addProperty("state", "");
            res.addProperty("info", "");
            res.addProperty("sent_time", "");
            res.addProperty("start_time", "");
            res.addProperty("finish_time", "");
        }

        return new Gson().toJson(res);

    }


    private String prepareDate(Date date) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss", Locale.ENGLISH);
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("CET"));
        return date == null ? "" : simpleDateFormat.format(date);
    }

    @Override
    public String getTaskNotification(String taskId) {
        Statement selectFromNotification = QueryBuilder.select().all()
                .from(CassandraTablesAndColumnsNames.NOTIFICATIONS_TABLE)
                .where(QueryBuilder.eq(CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID, Long.valueOf(taskId)));

        ResultSet notifications = cassandra.getSession().execute(selectFromNotification);

        return new Gson().toJson(notificationResultsetToJsonObject(notifications));
    }

    @Override
    public void incrTaskProgress(String taskId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private JsonArray notificationResultsetToJsonObject(ResultSet data) {
        JsonArray res = new JsonArray();

        for (Row row : data) {
            JsonObject line = new JsonObject();

            line.addProperty(CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID,
                    row.getLong(CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID));

            line.addProperty(CassandraTablesAndColumnsNames.NOTIFICATION_TOPOLOGY_NAME,
                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_TOPOLOGY_NAME));

            line.addProperty(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE,
                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE));

            line.addProperty(CassandraTablesAndColumnsNames.NOTIFICATION_STATE,
                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_STATE));

            line.addProperty(CassandraTablesAndColumnsNames.NOTIFICATION_INFO_TEXT,
                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_INFO_TEXT));

            line.addProperty(CassandraTablesAndColumnsNames.NOTIFICATION_ADDITIONAL_INFORMATIONS,
                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_ADDITIONAL_INFORMATIONS));

            line.addProperty(CassandraTablesAndColumnsNames.NOTIFICATION_RESULT_RESOURCE,
                    row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_RESULT_RESOURCE));

            res.add(line);
        }

        return res;
    }
}
