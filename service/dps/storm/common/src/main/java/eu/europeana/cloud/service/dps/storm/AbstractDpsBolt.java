package eu.europeana.cloud.service.dps.storm;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.utils.DiagnosticContextWrapper;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.apache.commons.lang.exception.ExceptionUtils;
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

  protected static final Logger STATISTICS_LOGGER = LoggerFactory.getLogger("STATISTICS_LOGGER");
  protected static final String STATISTICS_LOGGER_MESSAGE_PATTERN = "[{}],{},{}";

  public static final String NOTIFICATION_STREAM_NAME = "NotificationStream";

  // default number of retries
  public static final int DEFAULT_RETRIES = 3;

  public static final int SLEEP_TIME = 5000;
  protected final CassandraProperties cassandraProperties;

  protected transient TaskStatusChecker taskStatusChecker;

  protected transient Map<?, ?> stormConfig;
  protected transient TopologyContext topologyContext;
  protected transient OutputCollector outputCollector;
  protected String topologyName;

  public abstract void execute(Tuple anchorTuple, StormTaskTuple t);

  public abstract void prepare();

  protected boolean ignoreDeleted() {
    return true;
  }

  public AbstractDpsBolt(CassandraProperties cassandraProperties) {
    this.cassandraProperties = cassandraProperties;
  }

  @Override
  public void execute(Tuple tuple) {
    StormTaskTuple stormTaskTuple = null;
    try {
      stormTaskTuple = StormTaskTuple.fromStormTuple(tuple);
      LOGGER.debug("{} Performing execute on tuple {}", getClass().getName(), stormTaskTuple);
      prepareDiagnosticContext(stormTaskTuple);

      if (stormTaskTuple.getRecordAttemptNumber() > 1) {
        cleanInvalidData(stormTaskTuple);
      }

      if (taskStatusChecker.hasDroppedStatus(stormTaskTuple.getTaskId())) {
        outputCollector.fail(tuple);
        LOGGER.info("Interrupting execution cause task was dropped: {} recordId: {}",
            stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl());
        return;
      }

      if (ignoreDeleted() && stormTaskTuple.isMarkedAsDeleted()) {
        LOGGER.debug("Ignoring and passing further delete record with taskId {} and parameters list : {}",
            stormTaskTuple.getTaskId(), stormTaskTuple.getParameters());
        outputCollector.emit(tuple, stormTaskTuple.toStormTuple());
        outputCollector.ack(tuple);
        return;
      }

      LOGGER.debug("{} Mapped to StormTaskTuple with taskId {} and parameters list : {}",
          getClass().getName(), stormTaskTuple.getTaskId(), stormTaskTuple.getParameters());
      execute(tuple, stormTaskTuple);

    } catch (RetryInterruptedException e) {
      handleInterruption(e, tuple);
    } catch (Exception e) {
      if (Thread.currentThread().isInterrupted()) {
        handleInterruptedFlag(e, tuple);
      } else {
        handleException(tuple, stormTaskTuple, e);
      }
    } finally {
      LOGGER.debug("{} Ended execution.", getClass().getName());
      clearDiagnosticContext();
    }
  }

  private void handleException(Tuple tuple, StormTaskTuple stormTaskTuple, Exception e) {
    LOGGER.warn("{} error: {}", boltName(), e.getMessage(), e);
    if (stormTaskTuple != null) {
      var stack = new StringWriter();
      e.printStackTrace(new PrintWriter(stack));
      emitErrorNotification(tuple, stormTaskTuple, e.getMessage(), stack.toString());
      outputCollector.ack(tuple);
    }
  }

  protected void handleInterruptedFlag(Exception e, Tuple tuple) {
    LOGGER.error("{} thread was interrupted, and an exception caught: {}", boltName(), e.getMessage(), e);
    outputCollector.fail(tuple);
  }

  protected void handleInterruption(RetryInterruptedException e, Tuple tuple) {
    LOGGER.error("{} execution interrupted: {}", boltName(), e.getMessage(), e);
    outputCollector.fail(tuple);
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
    String hosts = cassandraProperties.getHosts();
    int port = cassandraProperties.getPort();
    String keyspaceName = cassandraProperties.getKeyspace();
    String userName = cassandraProperties.getUser();
    String password = cassandraProperties.getPassword();
    CassandraConnectionProvider cassandraConnectionProvider =
        CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
            hosts, port, keyspaceName, userName, password);
    taskStatusChecker = TaskStatusChecker.getTaskStatusChecker(cassandraConnectionProvider);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    //default stream
    declarer.declare(StormTaskTuple.getFields());

    //notifications
    declarer.declareStream(NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
  }

  protected void emitErrorNotification(Tuple anchorTuple, StormTaskTuple stormTaskTuple, String message, Throwable e) {
    emitErrorNotification(anchorTuple, stormTaskTuple, message,
        e.getMessage() + ":\n" + ExceptionUtils.getStackTrace(e));
  }

  protected void emitErrorNotification(Tuple anchorTuple, StormTaskTuple stormTaskTuple, String message) {
    emitErrorNotification(anchorTuple, stormTaskTuple, message, (String) null);
  }

  protected void emitErrorNotification(Tuple anchorTuple, StormTaskTuple stormTaskTuple, String message, String additionalInformation) {
    NotificationTuple nt = NotificationTuple.prepareNotificationWithResultResource(stormTaskTuple, RecordState.ERROR,
            message, additionalInformation);
    outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
  }

  protected void emitSuccessNotification(Tuple anchorTuple, StormTaskTuple stormTaskTuple,
                                         String message, String additionalInformation,
                                         String unifiedErrorMessage, String detailedErrorMessage) {
    NotificationTuple nt = NotificationTuple.prepareNotificationWithResultResourceAndErrorMessage(stormTaskTuple, RecordState.SUCCESS, message, additionalInformation, unifiedErrorMessage, detailedErrorMessage);
    outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
  }

  protected void emitSuccessNotification(Tuple anchorTuple, StormTaskTuple stormTaskTuple, String message, String additionalInformation) {
    NotificationTuple nt = NotificationTuple.prepareNotificationWithResultResource(stormTaskTuple, RecordState.SUCCESS, message, additionalInformation);
    outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
  }

  protected void emitSuccessNotification(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    emitSuccessNotification(anchorTuple, stormTaskTuple, "", "");
  }

  protected void emitIgnoredNotification(Tuple anchorTuple, StormTaskTuple stormTaskTuple,
                                         String message, String additionalInformation) {
    NotificationTuple tuple = NotificationTuple.prepareNotificationWithResultResource(stormTaskTuple, RecordState.SUCCESS, message, additionalInformation);
    tuple.addParameter(PluginParameterKeys.IGNORED_RECORD, "true");
    outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, tuple.toStormTuple());
  }

  protected void prepareStormTaskTupleForEmission(StormTaskTuple stormTaskTuple, String resultString)
          throws MalformedURLException {
    stormTaskTuple.setFileData(resultString.getBytes(StandardCharsets.UTF_8));
    final UrlParser urlParser = new UrlParser(stormTaskTuple.getFileUrl());
    stormTaskTuple.addParameter(PluginParameterKeys.CLOUD_ID, urlParser.getPart(UrlPart.RECORDS));
    stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_NAME, urlParser.getPart(UrlPart.REPRESENTATIONS));
    stormTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_VERSION, urlParser.getPart(UrlPart.VERSIONS));
  }

  protected void cleanInvalidData(StormTaskTuple tuple) {
    int attemptNumber = tuple.getRecordAttemptNumber();
    LOGGER.info("Attempt number {} to process this message. No cleaning done here.", attemptNumber);
    // nothing to clean here when the message is reprocessed
  }

  protected void logStatistics(LogStatisticsPosition position, String opName, String opId) {
    STATISTICS_LOGGER.debug(STATISTICS_LOGGER_MESSAGE_PATTERN, position, opName, opId);
  }

  private String boltName() {
    return getClass().getSimpleName();
  }

  public enum LogStatisticsPosition {
    BEGIN,
    END
  }
}
