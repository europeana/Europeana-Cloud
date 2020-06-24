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
import java.nio.charset.Charset;
import java.util.Map;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.*;
import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys.CASSANDRA_USERNAME;
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

    protected Map stormConfig;
    protected TopologyContext topologyContext;
    protected OutputCollector outputCollector;
    protected String topologyName;

    public abstract void execute(StormTaskTuple t);

    public abstract void prepare();

    @Override
    public void execute(Tuple tuple) {

        StormTaskTuple t = null;
        try {
            t = StormTaskTuple.fromStormTuple(tuple);
            if (!taskStatusChecker.hasKillFlag(t.getTaskId())) {
                LOGGER.debug("Mapped to StormTaskTuple with taskId {} and parameters list : {}", t.getTaskId(), t.getParameters());
                execute(t);
            }
        } catch (Exception e) {
            LOGGER.info("AbstractDpsBolt error: {} \nStackTrace: \n{}", e.getMessage(), e.getStackTrace());
            if (t != null) {
                StringWriter stack = new StringWriter();
                e.printStackTrace(new PrintWriter(stack));
                emitErrorNotification(t.getTaskId(), t.getFileUrl(), e.getMessage(), stack.toString());
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
        CassandraConnectionProvider cassandraConnectionProvider = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(hosts, port, keyspaceName,
                userName, password);
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

    protected void emitErrorNotification(long taskId, String resource, String message, String additionalInformations) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, RecordState.ERROR, message, additionalInformations);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }

    protected void emitSuccessNotification(long taskId, String resource,
                                           String message, String additionalInformation, String resultResource, String unifiedErrorMessage, String detailedErrorMessage) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, RecordState.SUCCESS, message, additionalInformation, resultResource);
        nt.addParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE, unifiedErrorMessage);
        nt.addParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE, detailedErrorMessage);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }


    protected void logAndEmitError(StormTaskTuple t, String message) {
        LOGGER.error(message);
        emitErrorNotification(t.getTaskId(), t.getFileUrl(), message, t.getParameters().toString());
    }

    protected void emitSuccessNotification(long taskId, String resource,
                                           String message, String additionalInformation, String resultResource) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, RecordState.SUCCESS, message, additionalInformation, resultResource);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }

    protected void emitSuccessNotificationForIndexing(long taskId, DataSetCleanerParameters dataSetCleanerParameters, String dpsURL,String authenticationHeader, String resource,
                                                      String message, String additionalInformation, String resultResource) {
        NotificationTuple nt = NotificationTuple.prepareIndexingNotification(taskId, dataSetCleanerParameters, dpsURL,authenticationHeader,
                resource, RecordState.SUCCESS, message, additionalInformation, resultResource);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }

    protected void prepareStormTaskTupleForEmission(StormTaskTuple stormTaskTuple, String resultString) throws MalformedURLException {
        stormTaskTuple.setFileData(resultString.getBytes(Charset.forName("UTF-8")));
        final UrlParser urlParser = new UrlParser(stormTaskTuple.getFileUrl());
        stormTaskTuple.addParameter(PluginParameterKeys.CLOUD_ID, urlParser.getPart(UrlPart.RECORDS));
        stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_NAME, urlParser.getPart(UrlPart.REPRESENTATIONS));
        stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_VERSION, urlParser.getPart(UrlPart.VERSIONS));
    }

    protected void waitForSpecificTime() {
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            LOGGER.error(e1.getMessage());
        }
    }

    protected void cleanInvalidData(long taskId, String recordId) {}
}