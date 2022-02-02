package eu.europeana.cloud.service.dps.storm.notification.handler;

import com.datastax.driver.core.BoundStatement;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.storm.BatchExecutor;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.dao.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * Handles {@link NotificationTuple} that is generated in case of error in bolts.
 * It also handles record that is not the last one record in the task (changes of the task status (for example to PROCESSED) are not needed here);
 *
 * <p>What has to be done here:</p>
 * <li>Insert row to <b>notifications</b> table</li>
 * <li>Update task counters in <b>taskInfo</b> table</li>
 * <li>Update record status in <b>processedRecords</b> table</li>
 * <li>Update error counters in <b>error_counters</b> table</li>
 * <li>Insert error information in <b>error_notifications</b> table</li>
 */

public class NotificationWithError extends NotificationTupleHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationWithError.class);

    public NotificationWithError(ProcessedRecordsDAO processedRecordsDAO,
                                 TaskDiagnosticInfoDAO taskDiagnosticInfoDAO,
                                 CassandraSubTaskInfoDAO subTaskInfoDAO,
                                 CassandraTaskErrorsDAO taskErrorDAO,
                                 CassandraTaskInfoDAO taskInfoDAO,
                                 TasksByStateDAO tasksByStateDAO,
                                 BatchExecutor batchExecutor,
                                 String topologyName) {
        super(processedRecordsDAO,
                taskDiagnosticInfoDAO,
                subTaskInfoDAO,
                taskErrorDAO,
                taskInfoDAO,
                tasksByStateDAO,
                batchExecutor,
                topologyName);
    }

    @Override
    protected List<BoundStatement> prepareStatementsForTupleContainingLastRecord(NotificationTuple notificationTuple) {
        return Collections.emptyList();
    }

    @Override
    protected List<BoundStatement> prepareStatementsForRecordState(NotificationTuple notificationTuple) {
        return prepareStatementsForRecordState(notificationTuple, RecordState.ERROR);
    }
}
