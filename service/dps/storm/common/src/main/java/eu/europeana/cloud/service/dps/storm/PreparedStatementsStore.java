package eu.europeana.cloud.service.dps.storm;

import com.datastax.driver.core.PreparedStatement;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.dps.storm.dao.CassandraDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;

import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.*;

/**
 *
 */
public class PreparedStatementsStore extends CassandraDAO {

    //NotificationsTable
    public PreparedStatement subtaskInsertStatement;

    //task_info table
    public PreparedStatement updateCounters;

    //processed_records table
    public PreparedStatement updateRecordStateStatement;

    //processed_records table
    public PreparedStatement updateLastRecordFinishedOnStormTime;


    public PreparedStatementsStore(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    @Override
    protected void prepareStatements() {
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
                + ") VALUES (?,?,?,?,?,?,?,?,?)");
        subtaskInsertStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        updateCounters = dbService.getSession().prepare(
                "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE + " SET "
                        + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_RECORDS_COUNT + " = ? , "
                        + CassandraTablesAndColumnsNames.TASK_INFO_IGNORED_RECORDS_COUNT + " = ? , "
                        + CassandraTablesAndColumnsNames.TASK_INFO_DELETED_RECORDS_COUNT + " = ? , "
                        + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_ERRORS_COUNT + " = ? , "
                        + CassandraTablesAndColumnsNames.TASK_INFO_DELETED_ERRORS_COUNT + " = ?" +
                        " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?");

        updateRecordStateStatement = dbService.getSession().prepare("INSERT INTO " + PROCESSED_RECORDS_TABLE +
                "("
                + PROCESSED_RECORDS_TASK_ID + ","
                + PROCESSED_RECORDS_RECORD_ID + ","
                + PROCESSED_RECORDS_BUCKET_NUMBER + ","
                + PROCESSED_RECORDS_STATE +
                ") VALUES (?,?,?,?)");

        updateLastRecordFinishedOnStormTime = prepare(String.format(
                "INSERT INTO %s(%s,%s) VALUES(?,?)",
                TASK_DIAGNOSTIC_INFO_TABLE, TASK_DIAGNOSTIC_INFO_ID, TASK_DIAGNOSTIC_INFO_LAST_RECORD_FINISHED_ON_STORM_TIME));
    }


}
