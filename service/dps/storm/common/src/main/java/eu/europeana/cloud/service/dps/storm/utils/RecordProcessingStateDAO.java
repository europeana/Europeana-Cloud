package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.RecordState;
import org.apache.commons.lang3.EnumUtils;

import java.util.Calendar;
import java.util.Optional;

import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.*;

public class RecordProcessingStateDAO extends CassandraDAO {
    private static final long TIME_TO_LIVE = 2 * 7 * 24 * 60 * 60;  //two weeks in seconds

    private PreparedStatement insertRecordStatement;
    private PreparedStatement selectRecordStatement;
    private PreparedStatement selectRecordStageStatement;
    private PreparedStatement updateRecordStageStatement;


    private RecordProcessingStateDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    private static RecordProcessingStateDAO instance = null;

    public static synchronized RecordProcessingStateDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = new RecordProcessingStateDAO(cassandra);
        }
        return instance;
    }

    @Override
    void prepareStatements() {
        insertRecordStatement = dbService.getSession().prepare("INSERT INTO " + RECORD_PROCESSING_STATE +
                "("
                + RECORD_PROCESSING_STATE_TASK_ID + ","
                + RECORD_PROCESSING_STATE_RECORD_ID + ","
                + RECORD_PROCESSING_STATE_ATTEMPT_NUMBER + ","
                + RECORD_PROCESSING_STATE_START_TIME +
                ") VALUES (?,?,?,?) USING TTL " + Long.toString(TIME_TO_LIVE)
        );

        selectRecordStatement = dbService.getSession().prepare("SELECT " +
                RECORD_PROCESSING_STATE_ATTEMPT_NUMBER +
                " FROM " + RECORD_PROCESSING_STATE +
                " WHERE " +
                RECORD_PROCESSING_STATE_TASK_ID + " = ? AND " +
                RECORD_PROCESSING_STATE_RECORD_ID + " = ? "
        );

        selectRecordStageStatement = dbService.getSession().prepare("SELECT " +
                RECORD_PROCESSING_STATE_STAGE +
                " FROM " + RECORD_PROCESSING_STATE +
                " WHERE " +
                RECORD_PROCESSING_STATE_TASK_ID + " = ? AND " +
                RECORD_PROCESSING_STATE_RECORD_ID + " = ? "
        );

        updateRecordStageStatement = dbService.getSession().prepare("INSERT INTO " + RECORD_PROCESSING_STATE +
                "("
                + RECORD_PROCESSING_STATE_TASK_ID + ","
                + RECORD_PROCESSING_STATE_RECORD_ID + ","
                + RECORD_PROCESSING_STATE_STAGE +
                ") VALUES (?,?,?) USING TTL " + TIME_TO_LIVE
        );
    }


    public void insertProcessingRecord(long taskId, String recordId, int attemptNumber) {
        dbService.getSession().execute(
                insertRecordStatement.bind(taskId, recordId, attemptNumber, Calendar.getInstance().getTime())
        );
    }

    public int selectProcessingRecordAttempt(long taskId, String srcIdentifier) {
        int result = 0;

        ResultSet rs = dbService.getSession().execute(selectRecordStatement.bind(taskId, srcIdentifier));
        Row row = rs.one();
        if (row != null) {
            result = row.getInt(RECORD_PROCESSING_STATE_ATTEMPT_NUMBER);
        }

        return result;
    }

    public Optional<RecordState> selectProcessingRecordStage(long taskId, String srcIdentifier) {
        ResultSet rs = dbService.getSession().execute(selectRecordStageStatement.bind(taskId, srcIdentifier));
        Row row = rs.one();
        String stageFieldValue = row.getString(RECORD_PROCESSING_STATE_STAGE);
        return Optional.ofNullable(EnumUtils.getEnum(RecordState.class, stageFieldValue));
    }

    public void updateProcessingRecordStage(long taskId, String recordId, String stage) {
        dbService.getSession().execute(
                updateRecordStageStatement.bind(taskId, recordId, stage));
    }
}
