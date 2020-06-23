package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;

import java.util.Calendar;
import java.util.Date;

import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.*;

public class RecordProcessingStateDAO extends CassandraDAO {
    private static final long TIME_TO_LIVE = 2 * 7 * 24 * 60 * 60;  //two weeks in seconds

    private PreparedStatement insertRecordStatement;
    private PreparedStatement selectRecordStatement;

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
                + RECORD_PROCESSING_STATE_TOPOLOGY_NAME + ","
                + RECORD_PROCESSING_STATE_ATTEMPT_NUMBER + ","
                + RECORD_PROCESSING_STATE_START_TIME +
                ") VALUES (?,?,?,?,?) USING TTL " + Long.toString(TIME_TO_LIVE)
        );

        selectRecordStatement = dbService.getSession().prepare("SELECT " +
                RECORD_PROCESSING_STATE_ATTEMPT_NUMBER +
                " FROM " + RECORD_PROCESSING_STATE +
                " WHERE " +
                RECORD_PROCESSING_STATE_TASK_ID + " = ? AND " +
                RECORD_PROCESSING_STATE_RECORD_ID + " = ? AND " +
                RECORD_PROCESSING_STATE_TOPOLOGY_NAME + " = ?"
        );
    }


    public void insertProcessingRecord(long taskId, String recordId, String topologyName, int attemptNumber)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(
                insertRecordStatement.bind(taskId, recordId, topologyName, attemptNumber, Calendar.getInstance().getTime())
        );
    }

    public int selectProcessingRecordAttempt(long taskId, String srcIdentifier, String topologyName)
            throws NoHostAvailableException, QueryExecutionException {

        int result = 0;

        ResultSet rs = dbService.getSession().execute(selectRecordStatement.bind(taskId, srcIdentifier, topologyName));
        Row row = rs.one();
        if (row != null) {
            result = row.getInt(RECORD_PROCESSING_STATE_ATTEMPT_NUMBER);
        }

        return result;
    }

}
