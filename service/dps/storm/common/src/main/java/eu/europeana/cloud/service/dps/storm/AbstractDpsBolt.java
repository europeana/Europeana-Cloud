package eu.europeana.cloud.service.dps.storm;


import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
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

    protected static volatile TaskStatusChecker taskStatusChecker;
    public static final String NOTIFICATION_STREAM_NAME = "NotificationStream";
    protected static final String AUTHORIZATION = "Authorization";


    // default number of retries
    public static final int DEFAULT_RETRIES = 3;

    public static final int SLEEP_TIME = 5000;

    protected transient Map stormConfig;
    protected transient TopologyContext topologyContext;
    protected transient OutputCollector outputCollector;
    protected String topologyName;

    public abstract void execute(Tuple anchorTuple, StormTaskTuple t);

    public abstract void prepare();

    @Override
    public void execute(Tuple tuple) {
        StormTaskTuple stormTaskTuple = null;
        try {
            stormTaskTuple = StormTaskTuple.fromStormTuple(tuple);

            if(stormTaskTuple.getRecordAttemptNumber() > 1) {
                cleanInvalidData(stormTaskTuple);
            }

            if (!taskStatusChecker.hasKillFlag(stormTaskTuple.getTaskId())) {
                LOGGER.debug("Mapped to StormTaskTuple with taskId {} and parameters list : {}", stormTaskTuple.getTaskId(), stormTaskTuple.getParameters());
                execute(tuple, stormTaskTuple);
            }
        } catch (Exception e) {
            LOGGER.info("AbstractDpsBolt error: {} \nStackTrace: \n{}", e.getMessage(), e.getStackTrace());
            if (stormTaskTuple != null) {
                StringWriter stack = new StringWriter();
                e.printStackTrace(new PrintWriter(stack));
                emitErrorNotification(tuple, stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), e.getMessage(), stack.toString());
            }
        }
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

    protected void emitErrorNotification(Tuple anchorTuple, long taskId, String resource, String message, String additionalInformations) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, RecordState.ERROR, message, additionalInformations);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
        outputCollector.ack(anchorTuple);
    }

    protected void emitSuccessNotification(Tuple anchorTuple, long taskId, String resource,
                                           String message, String additionalInformation, String resultResource, String unifiedErrorMessage, String detailedErrorMessage) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, RecordState.SUCCESS, message, additionalInformation, resultResource);
        nt.addParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE, unifiedErrorMessage);
        nt.addParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE, detailedErrorMessage);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
        outputCollector.ack(anchorTuple);
    }

    /**
     * @deprecated
     */
    @Deprecated
    protected void logAndEmitError(Tuple anchorTuple, StormTaskTuple t, String message) {
        LOGGER.error(message);
        emitErrorNotification(anchorTuple, t.getTaskId(), t.getFileUrl(), message, t.getParameters().toString());
    }

    protected void emitSuccessNotification(Tuple anchorTuple, long taskId, String resource,
                                           String message, String additionalInformation, String resultResource) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, RecordState.SUCCESS, message, additionalInformation, resultResource);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
        outputCollector.ack(anchorTuple);
    }

    protected void emitSuccessNotificationForIndexing(Tuple anchorTuple, long taskId, DataSetCleanerParameters dataSetCleanerParameters, String dpsURL,String authenticationHeader, String resource,
                                                      String message, String additionalInformation, String resultResource) {
        NotificationTuple nt = NotificationTuple.prepareIndexingNotification(taskId, dataSetCleanerParameters, dpsURL,authenticationHeader,
                resource, RecordState.SUCCESS, message, additionalInformation, resultResource);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
        outputCollector.ack(anchorTuple);
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
        //If there is some data to clean for given bolt and tuple -
        //overwrite this method in bold and process data for given tuple
    }
}