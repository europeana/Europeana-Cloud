package eu.europeana.cloud.service.dps.storm.dao;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.*;

public class ProcessedRecordsDAOTest extends CassandraTestBase {

    private ProcessedRecordsDAO processedRecordsDAO;

    @Before
    public void setup() {
        CassandraConnectionProvider db = new CassandraConnectionProvider(HOST, CassandraTestInstance.getPort(), KEYSPACE, USER, PASSWORD);
        processedRecordsDAO = ProcessedRecordsDAO.getInstance(db);
    }

    @Test
    public void shouldInsertSameRowOnlyOneTime() {
        boolean firstAttempt = processedRecordsDAO.insertIfNotExists(
                1L,
                "recordID",
                1,
                "dstResource",
                "topologyName",
                RecordState.SUCCESS.toString(),
                "infoText",
                "additionalInformation");
        boolean secondAttempt = processedRecordsDAO.insertIfNotExists(
                1L,
                "recordID",
                1,
                "dstResource",
                "topologyName",
                RecordState.QUEUED.toString(),
                "infoText",
                "additionalInformation");

        assertTrue(firstAttempt);
        assertFalse(secondAttempt);

        Optional<ProcessedRecord> theRecord = processedRecordsDAO.selectByPrimaryKey(1L, "recordID");
        assertSame(RecordState.SUCCESS, theRecord.get().getState());
    }

    @Test
    public void shouldNotChangeRowContentsInCaseOfSecondConditionalInsert() {
        processedRecordsDAO.insertIfNotExists(
                1L,
                "recordID",
                1,
                "dstResource",
                "topologyName",
                RecordState.SUCCESS.toString(),
                "infoText",
                "additionalInformation");

        processedRecordsDAO.insertIfNotExists(
                1L,
                "recordID",
                1,
                "dstResource",
                "topologyName",
                RecordState.QUEUED.toString(),
                "infoText",
                "additionalInformation");

        Optional<ProcessedRecord> theRecord = processedRecordsDAO.selectByPrimaryKey(1L, "recordID");
        assertSame(RecordState.SUCCESS, theRecord.get().getState());
    }
}