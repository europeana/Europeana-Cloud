package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;

import java.util.Calendar;
import java.util.Optional;

import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.*;

/**
 * DAO for processing data in  {@link CassandraTablesAndColumnsNames#PROCESSED_RECORDS_TOPOLOGY_NAME}
 */
public class ProcessedRecordsDAO extends CassandraDAO {
    private static final long TIME_TO_LIVE = 2 * 7 * 24 * 60 * 60L;  //two weeks in seconds

    private PreparedStatement insertStatement;
    private PreparedStatement updateRecordStateStatement;
    private PreparedStatement selectByPrimaryKeyStatement;

    private static ProcessedRecordsDAO instance = null;

    public static synchronized ProcessedRecordsDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = new ProcessedRecordsDAO(cassandra);
        }
        return instance;
    }

    public ProcessedRecordsDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    @Override
    void prepareStatements() {
        insertStatement = dbService.getSession().prepare("INSERT INTO " + PROCESSED_RECORDS_TABLE +
                "("
                + PROCESSED_RECORDS_TASK_ID + ","
                + PROCESSED_RECORDS_RECORD_ID + ","
                + PROCESSED_RECORDS_ATTEMPT_NUMBER + ","
                + PROCESSED_RECORDS_DST_IDENTIFIER + ","
                + PROCESSED_RECORDS_TOPOLOGY_NAME + ","
                + PROCESSED_RECORDS_STATE + ","
                + PROCESSED_RECORDS_START_TIME + ","
                + PROCESSED_RECORDS_INFO_TEXT + ","
                + PROCESSED_RECORDS_ADDITIONAL_INFORMATIONS +
                ") VALUES (?,?,?,?,?,?,?,?,?) USING TTL " + TIME_TO_LIVE);

        updateRecordStateStatement = dbService.getSession().prepare("INSERT INTO " + PROCESSED_RECORDS_TABLE +
                "("
                + PROCESSED_RECORDS_TASK_ID + ","
                + PROCESSED_RECORDS_RECORD_ID + ","
                + PROCESSED_RECORDS_STATE +
                ") VALUES (?,?,?) USING TTL " + TIME_TO_LIVE);

        selectByPrimaryKeyStatement = dbService.getSession().prepare("SELECT "
                + PROCESSED_RECORDS_ATTEMPT_NUMBER + ","
                + PROCESSED_RECORDS_DST_IDENTIFIER + ","
                + PROCESSED_RECORDS_TOPOLOGY_NAME + ","
                + PROCESSED_RECORDS_STATE + ","
                + PROCESSED_RECORDS_START_TIME + ","
                + PROCESSED_RECORDS_INFO_TEXT + ","
                + PROCESSED_RECORDS_ADDITIONAL_INFORMATIONS +
                " FROM " + PROCESSED_RECORDS_TABLE + " WHERE " + PROCESSED_RECORDS_TASK_ID + " = ? AND " + PROCESSED_RECORDS_RECORD_ID + " = ?");

    }

    public void insert(long taskId, String recordId, int attemptNumber, String dstResource, String topologyName,
                       String state, String infoText, String additionalInformations)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(insertStatement.bind(taskId, recordId, attemptNumber, dstResource, topologyName,
                state, Calendar.getInstance().getTime(), infoText, additionalInformations));
    }

    public void insert(ProcessedRecord record)
            throws NoHostAvailableException, QueryExecutionException {
        insert(record.getTaskId(), record.getRecordId(), record.getAttemptNumber(), record.getDstIdentifier(),
                record.getTopologyName(), record.getState().toString(), record.getInfoText(),
                record.getAdditionalInformations());
    }

    public void updateProcessedRecordState(long taskId, String recordId, String stage) {
        dbService.getSession().execute(
                updateRecordStateStatement.bind(taskId, recordId, stage));
    }

    public Optional<ProcessedRecord> selectByPrimaryKey(long taskId, String recordId)
            throws NoHostAvailableException, QueryExecutionException {
        ProcessedRecord result = null;

        ResultSet rs = dbService.getSession().execute(selectByPrimaryKeyStatement.bind(taskId, recordId));
        Row row = rs.one();
        if (row != null) {
            result = ProcessedRecord
                    .builder()
                    .taskId(taskId)
                    .recordId(recordId)
                    .attemptNumber(row.getInt(PROCESSED_RECORDS_ATTEMPT_NUMBER))
                    .dstIdentifier(row.getString(PROCESSED_RECORDS_DST_IDENTIFIER))
                    .topologyName(row.getString(PROCESSED_RECORDS_TOPOLOGY_NAME))
                    .state(RecordState.valueOf(row.getString(PROCESSED_RECORDS_STATE)))
                    .starTime(row.getTimestamp(PROCESSED_RECORDS_START_TIME))
                    .infoText(row.getString(PROCESSED_RECORDS_INFO_TEXT))
                    .additionalInformations(row.getString(PROCESSED_RECORDS_ADDITIONAL_INFORMATIONS))
                    .build();
        }

        return Optional.ofNullable(result);
    }

    public int getAttemptNumber(long taskId, String recordId) {
        return selectByPrimaryKey(taskId, recordId).map(ProcessedRecord::getAttemptNumber).orElse(0);
    }
}
