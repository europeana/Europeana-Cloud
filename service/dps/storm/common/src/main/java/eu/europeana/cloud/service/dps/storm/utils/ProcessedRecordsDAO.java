package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;

import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.*;

/**
 * DAO for processing data in  {@link CassandraTablesAndColumnsNames#PROCESSED_RECORDS_TOPOLOGY_NAME}
 */
public class ProcessedRecordsDAO extends CassandraDAO {
    private PreparedStatement insertStatement;
    private PreparedStatement selectByPrimaryKeyStatement;

    public ProcessedRecordsDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    @Override
    void prepareStatements() {
        insertStatement = dbService.getSession().prepare("INSERT INTO " + PROCESSED_RECORDS_TABLE +
                "("
                + PROCESSED_RECORDS_TASK_ID + ","
                + PROCESSED_RECORDS_SRC_IDENTIFIER + ","
                + PROCESSED_RECORDS_DST_IDENTIFIER + ","
                + PROCESSED_RECORDS_TOPOLOGY_NAME + ","
                + PROCESSED_RECORDS_STATE + ","
                + PROCESSED_RECORDS_INFO_TEXT + ","
                + PROCESSED_RECORDS_ADDITIONAL_INFORMATIONS +
                ") VALUES (?,?,?,?,?,?,?)");

        selectByPrimaryKeyStatement = dbService.getSession().prepare("SELECT "
                + PROCESSED_RECORDS_DST_IDENTIFIER + ","
                + PROCESSED_RECORDS_TOPOLOGY_NAME + ","
                + PROCESSED_RECORDS_STATE + ","
                + PROCESSED_RECORDS_INFO_TEXT + ","
                + PROCESSED_RECORDS_ADDITIONAL_INFORMATIONS +
                " FROM " + PROCESSED_RECORDS_TABLE + " WHERE "+PROCESSED_RECORDS_TASK_ID+" = ? AND "+ PROCESSED_RECORDS_SRC_IDENTIFIER +" = ?");

    }

    public void insert(long taskId, String srcResource, String dstResource, String topologyName,
                       String state, String infoText, String additionalInformations)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(insertStatement.bind(taskId, srcResource, dstResource, topologyName,
                state, infoText, additionalInformations));
    }

    public ProcessedRecord selectByPrimaryKey(long taskId, String srcIdentifier)
            throws NoHostAvailableException, QueryExecutionException {
        ProcessedRecord result = null;

        ResultSet rs = dbService.getSession().execute(selectByPrimaryKeyStatement.bind(taskId, srcIdentifier));
        Row row = rs.one();
        if(row != null) {
            result = ProcessedRecord
                    .builder()
                    .taskId(taskId)
                    .srcIdentifier(srcIdentifier)
                    .dstIdentifier(row.getString(PROCESSED_RECORDS_DST_IDENTIFIER))
                    .topologyName(row.getString(PROCESSED_RECORDS_TOPOLOGY_NAME))
                    .state(RecordState.valueOf(row.getString(PROCESSED_RECORDS_STATE)))
                    .infoText(row.getString(PROCESSED_RECORDS_INFO_TEXT))
                    .additionalInformations(row.getString(PROCESSED_RECORDS_ADDITIONAL_INFORMATIONS))
                    .build();
        }

        return result;
    }
}
