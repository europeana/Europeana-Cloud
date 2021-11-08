package eu.europeana.cloud.service.dps.storm;


import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.storm.utils.DiagnosticContextWrapper;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static java.lang.Integer.parseInt;

/**
 * Abstract class for all Storm bolts used in Europeana Cloud.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public abstract class AbstractDpsBolt extends BaseRichBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDpsBolt.class);

    public static final String NOTIFICATION_STREAM_NAME = "NotificationStream";
    protected static final String AUTHORIZATION = "Authorization";

    // default number of retries
    public static final int DEFAULT_RETRIES = 3;

    public static final int SLEEP_TIME = 5000;

    protected transient TaskStatusChecker taskStatusChecker;

    protected transient Map<?,?> stormConfig;
    protected transient TopologyContext topologyContext;
    protected transient OutputCollector outputCollector;
    protected String topologyName;

    public abstract void execute(Tuple anchorTuple, StormTaskTuple t);

    public abstract void prepare();

    protected boolean ignoreDeleted(){
        return true;
    }

    @Override
    public void execute(Tuple tuple) {
        StormTaskTuple stormTaskTuple = null;
        try {
            stormTaskTuple = StormTaskTuple.fromStormTuple(tuple);
            LOGGER.debug("{} Performing execute on tuple {}", getClass().getName(),stormTaskTuple);
            prepareDiagnosticContext(stormTaskTuple);

            if(stormTaskTuple.getRecordAttemptNumber() > 1) {
                cleanInvalidData(stormTaskTuple);
            }

            if (taskStatusChecker.hasDroppedStatus(stormTaskTuple.getTaskId())) {
                outputCollector.fail(tuple);
                LOGGER.info("Interrupting execution cause task was dropped: {} recordId: {}", stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl());
                return;
            }

            if(ignoreDeleted() && stormTaskTuple.isMarkedAsDeleted()){
                LOGGER.debug("Ingornigng and passing further delete record with taskId {} and parameters list : {}", stormTaskTuple.getTaskId(), stormTaskTuple.getParameters());
                outputCollector.emit(tuple, stormTaskTuple.toStormTuple());
                outputCollector.ack(tuple);
                return;
            }

            LOGGER.debug("{} Mapped to StormTaskTuple with taskId {} and parameters list : {}", getClass().getName(),
                    stormTaskTuple.getTaskId(), stormTaskTuple.getParameters());
            execute(tuple, stormTaskTuple);

        } catch (Exception e) {
            LOGGER.info("AbstractDpsBolt error: {}", e.getMessage(), e);
            if (stormTaskTuple != null) {
                var stack = new StringWriter();
                e.printStackTrace(new PrintWriter(stack));
                emitErrorNotification(tuple, stormTaskTuple.getTaskId(), stormTaskTuple.isMarkedAsDeleted(),
                        stormTaskTuple.getFileUrl(), e.getMessage(), stack.toString(),
                        StormTaskTupleHelper.getRecordProcessingStartTime(stormTaskTuple));
                outputCollector.ack(tuple);
            }
        } finally {
            clearDiagnosticContext();
        }
    }

    private void prepareDiagnosticContext(StormTaskTuple stormTaskTuple) {
        DiagnosticContextWrapper.putValuesFrom(stormTaskTuple);
    }

    private void clearDiagnosticContext() {
        DiagnosticContextWrapper.clear();
    }

    @Override
    public void prepare(Map stormConfig, TopologyContext tc, OutputCollector oc) {
        this.stormConfig = stormConfig;
        this.topologyContext = tc;
        this.outputCollector = oc;
        this.topologyName = (String) stormConfig.get(Config.TOPOLOGY_NAME);
        initTaskStatusChecker();
        prepare();
    }

    private void initTaskStatusChecker() {
        String hosts = (String) stormConfig.get(CASSANDRA_HOSTS);
        int port = parseInt((String) stormConfig.get(CASSANDRA_PORT));
        String keyspaceName = (String) stormConfig.get(CASSANDRA_KEYSPACE_NAME);
        String userName = (String) stormConfig.get(CASSANDRA_USERNAME);
        String password = (String) stormConfig.get(CASSANDRA_SECRET_TOKEN);
        CassandraConnectionProvider cassandraConnectionProvider =
                CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
                        hosts, port, keyspaceName, userName, password);

        synchronized (AbstractDpsBolt.class) {
            if (taskStatusChecker == null) {
                try {
                    TaskStatusChecker.init(cassandraConnectionProvider);
                } catch (IllegalStateException e) {
                    LOGGER.info("It was already initialized Before");
                }
                taskStatusChecker = TaskStatusChecker.getTaskStatusChecker();
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //default stream
        declarer.declare(StormTaskTuple.getFields());

        //notifications
        declarer.declareStream(NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
    }


    /**
     * Emit {@link NotificationTuple} with error notification to {@link #NOTIFICATION_STREAM_NAME}.
     * Only one notification call per resource per task.
     *
     * @param taskId                 task ID
     * @param resource               affected resource (e.g. file URL)
     * @param message                short text
     * @param additionalInformations the rest of informations (e.g. stack trace)
     */

    protected void emitErrorNotification(Tuple anchorTuple, long taskId, boolean markedAsDeleted, String resource,
                                         String message, String additionalInformations, long processingStartTime) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId, markedAsDeleted, resource, RecordState.ERROR,
                message, additionalInformations, processingStartTime);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
    }

    protected void emitSuccessNotification(Tuple anchorTuple, long taskId, boolean markedAsDelete, String resource,
                                           String message, String additionalInformation, String resultResource,
                                           String unifiedErrorMessage, String detailedErrorMessage, long processingStartTime) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId, markedAsDelete,
                resource, RecordState.SUCCESS, message, additionalInformation, resultResource, processingStartTime);
        nt.addParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE, unifiedErrorMessage);
        nt.addParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE, detailedErrorMessage);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
    }

    protected void emitSuccessNotification(Tuple anchorTuple, long taskId, boolean markedAsDelete, String resource,
                                           String message, String additionalInformation, String resultResource,
                                           long processingStartTime) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId, markedAsDelete,
                resource, RecordState.SUCCESS, message, additionalInformation, resultResource, processingStartTime);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
    }

    protected void emitIgnoredNotification(Tuple anchorTuple, long taskId, boolean markedAsDeleted, String resource,
                                           String message, String additionalInformation,
                                           long processingStartTime) {
        NotificationTuple tuple = NotificationTuple.prepareNotification(taskId, markedAsDeleted,
                resource, RecordState.SUCCESS, message, additionalInformation, "", processingStartTime);
        tuple.addParameter(PluginParameterKeys.IGNORED_RECORD, "true");
        outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, tuple.toStormTuple());
    }


    protected void emitSuccessNotificationForIndexing(Tuple anchorTuple, long taskId, boolean markedAsDeleted,
                                                      DataSetCleanerParameters dataSetCleanerParameters,
                                                      String authenticationHeader, String resource, String message,
                                                      String additionalInformation, String resultResource,
                                                      long processingStartTime) {
        NotificationTuple nt = NotificationTuple.prepareIndexingNotification(taskId, markedAsDeleted, dataSetCleanerParameters, authenticationHeader,
                resource, RecordState.SUCCESS, message, additionalInformation, resultResource, processingStartTime);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
    }

    protected void prepareStormTaskTupleForEmission(StormTaskTuple stormTaskTuple, String resultString) throws MalformedURLException {
        stormTaskTuple.setFileData(resultString.getBytes(StandardCharsets.UTF_8));
        final UrlParser urlParser = new UrlParser(stormTaskTuple.getFileUrl());
        stormTaskTuple.addParameter(PluginParameterKeys.CLOUD_ID, urlParser.getPart(UrlPart.RECORDS));
        stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_NAME, urlParser.getPart(UrlPart.REPRESENTATIONS));
        stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_VERSION, urlParser.getPart(UrlPart.VERSIONS));
    }

    protected void waitForSpecificTime() {
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOGGER.error(ie.getMessage());
        }
    }

    protected void cleanInvalidData(StormTaskTuple tuple) {
        int attemptNumber = tuple.getRecordAttemptNumber();
        LOGGER.info("Attempt number {} to process this message. No cleaning done here.", attemptNumber);
        // nothing to clean here when the message is reprocessed
    }
}