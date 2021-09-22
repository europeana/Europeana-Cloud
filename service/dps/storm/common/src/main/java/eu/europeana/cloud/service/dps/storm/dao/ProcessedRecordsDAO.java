package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.utils.BucketUtils;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;

import java.util.Calendar;
import java.util.Date;
import java.util.Optional;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.*;

/**
 * DAO for processing data in  {@link CassandraTablesAndColumnsNames#PROCESSED_RECORDS_TOPOLOGY_NAME}
 */
@Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
public class ProcessedRecordsDAO extends CassandraDAO {
    private static final int BUCKETS_COUNT = 128;

    private PreparedStatement insertStatement;
    private PreparedStatement updateRecordStateStatement;
    private PreparedStatement updateRecordStartTime;
    private PreparedStatement updateAttemptNumberStatement;
    private PreparedStatement selectByPrimaryKeyStatement;

    private static ProcessedRecordsDAO instance = null;

    public ProcessedRecordsDAO(){
        //needed for creating cglib proxy in RetryableMethodExecutor.createRetryProxy()
    }

    public static synchronized ProcessedRecordsDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = RetryableMethodExecutor.createRetryProxy(new ProcessedRecordsDAO(cassandra));
        }
        return instance;
    }

    public ProcessedRecordsDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    @Override
    protected void prepareStatements() {
        insertStatement = dbService.getSession().prepare("INSERT INTO " + PROCESSED_RECORDS_TABLE +
                "("
                + PROCESSED_RECORDS_TASK_ID + ","
                + PROCESSED_RECORDS_RECORD_ID + ","
                + PROCESSED_RECORDS_BUCKET_NUMBER + ","
                + PROCESSED_RECORDS_ATTEMPT_NUMBER + ","
                + PROCESSED_RECORDS_DST_IDENTIFIER + ","
                + PROCESSED_RECORDS_TOPOLOGY_NAME + ","
                + PROCESSED_RECORDS_STATE + ","
                + PROCESSED_RECORDS_START_TIME + ","
                + PROCESSED_RECORDS_INFO_TEXT + ","
                + PROCESSED_RECORDS_ADDITIONAL_INFORMATIONS +
                ") VALUES (?,?,?,?,?,?,?,?,?,?)");

        updateRecordStateStatement = dbService.getSession().prepare("INSERT INTO " + PROCESSED_RECORDS_TABLE +
                "("
                + PROCESSED_RECORDS_TASK_ID + ","
                + PROCESSED_RECORDS_RECORD_ID + ","
                + PROCESSED_RECORDS_BUCKET_NUMBER + ","
                + PROCESSED_RECORDS_STATE +
                ") VALUES (?,?,?,?)");

        updateRecordStartTime = dbService.getSession().prepare("INSERT INTO " + PROCESSED_RECORDS_TABLE +
                "("
                + PROCESSED_RECORDS_TASK_ID + ","
                + PROCESSED_RECORDS_RECORD_ID + ","
                + PROCESSED_RECORDS_BUCKET_NUMBER + ","
                + PROCESSED_RECORDS_START_TIME +
                ") VALUES (?,?,?,?)");

        updateAttemptNumberStatement = dbService.getSession().prepare("INSERT INTO " + PROCESSED_RECORDS_TABLE +
                "("
                + PROCESSED_RECORDS_TASK_ID + ","
                + PROCESSED_RECORDS_RECORD_ID + ","
                + PROCESSED_RECORDS_BUCKET_NUMBER + ","
                + PROCESSED_RECORDS_ATTEMPT_NUMBER +
                ") VALUES (?,?,?,?)");

        selectByPrimaryKeyStatement = dbService.getSession().prepare("SELECT "
                + PROCESSED_RECORDS_ATTEMPT_NUMBER + ","
                + PROCESSED_RECORDS_DST_IDENTIFIER + ","
                + PROCESSED_RECORDS_TOPOLOGY_NAME + ","
                + PROCESSED_RECORDS_STATE + ","
                + PROCESSED_RECORDS_START_TIME + ","
                + PROCESSED_RECORDS_INFO_TEXT + ","
                + PROCESSED_RECORDS_ADDITIONAL_INFORMATIONS +
                " FROM " + PROCESSED_RECORDS_TABLE + " WHERE " + PROCESSED_RECORDS_TASK_ID + " = ? AND " + PROCESSED_RECORDS_RECORD_ID + " = ? AND " + PROCESSED_RECORDS_BUCKET_NUMBER + " = ?");

    }

    public void insert(long taskId, String recordId, int attemptNumber, String dstResource, String topologyName,
                       String state, String infoText, String additionalInformations)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(insertStatement.bind(taskId, recordId,
                BucketUtils.bucketNumber(recordId, BUCKETS_COUNT), attemptNumber, dstResource, topologyName,
                state, Calendar.getInstance().getTime(), infoText, additionalInformations));
    }

    public void insert(ProcessedRecord theRecord)
            throws NoHostAvailableException, QueryExecutionException {
        insert(theRecord.getTaskId(), theRecord.getRecordId(), theRecord.getAttemptNumber(), theRecord.getDstIdentifier(),
                theRecord.getTopologyName(), theRecord.getState().toString(), theRecord.getInfoText(),
                theRecord.getAdditionalInformations());
    }

    public void updateProcessedRecordState(long taskId, String recordId, RecordState state) {
        dbService.getSession().execute(
                updateRecordStateStatement.bind(taskId, recordId, BucketUtils.bucketNumber(recordId, BUCKETS_COUNT), state.toString()));
    }

    public Optional<ProcessedRecord> selectByPrimaryKey(long taskId, String recordId)
            throws NoHostAvailableException, QueryExecutionException {
        ProcessedRecord result = null;

        ResultSet rs = dbService.getSession().execute(selectByPrimaryKeyStatement.bind(taskId, recordId, BucketUtils.bucketNumber(recordId, BUCKETS_COUNT)));
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

    public void updateStartTime(long taskId, String recordId, Date startTime) {
        dbService.getSession().execute(
                updateRecordStartTime.bind(taskId, recordId, BucketUtils.bucketNumber(recordId, BUCKETS_COUNT), startTime));
    }

    public void updateAttempNumber(long taskId, String recordId, int attempNumber) {
        dbService.getSession().execute(
                updateAttemptNumberStatement.bind(taskId, recordId, BucketUtils.bucketNumber(recordId, BUCKETS_COUNT), attempNumber));
    }

}
