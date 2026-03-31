package eu.europeana.cloud.service.dps.storm;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.tuple.notification.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
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

  public abstract void execute(Tuple anchorTuple, CommonTaskTuple t);

  public abstract void prepare();

  protected boolean ignoreDeleted() {
    return true;
  }

  public AbstractDpsBolt(CassandraProperties cassandraProperties) {
    this.cassandraProperties = cassandraProperties;
  }

  @Override
  public void execute(Tuple tuple) {
    CommonTaskTuple commonTaskTuple = null;
    try {
      commonTaskTuple = CommonTaskTuple.fromStormTuple(tuple);
      LOGGER.debug("{} Performing execute on tuple {}", getClass().getName(), commonTaskTuple);
      prepareDiagnosticContext(commonTaskTuple);

      if (commonTaskTuple.getRecordAttemptNumber() > 1) {
        cleanInvalidData(commonTaskTuple);
      }

      if (taskStatusChecker.hasDroppedStatus(commonTaskTuple.getTaskId())) {
        outputCollector.fail(tuple);
        LOGGER.info("Interrupting execution cause task was dropped: {} recordId: {}",
                commonTaskTuple.getTaskId(), commonTaskTuple.getRecordUri());
        return;
      }

      if (ignoreDeleted() && commonTaskTuple.isMarkedAsDeleted()) {
        LOGGER.debug("Ignoring and passing further delete record with taskId {} and parameters list : {}",
            commonTaskTuple.getTaskId(), commonTaskTuple.getParameters());
        outputCollector.emit(tuple, commonTaskTuple.toStormTuple());
        outputCollector.ack(tuple);
        return;
      }

      LOGGER.debug("{} Mapped to CommonTaskTuple with taskId {} and parameters list : {}",
          getClass().getName(), commonTaskTuple.getTaskId(), commonTaskTuple.getParameters());
      execute(tuple, commonTaskTuple);

    } catch (RetryInterruptedException e) {
      handleInterruption(e, tuple);
    } catch (Exception e) {
      if (Thread.currentThread().isInterrupted()) {
        handleInterruptedFlag(e, tuple);
      } else {
        handleException(tuple, commonTaskTuple, e);
      }
    } finally {
      LOGGER.debug("{} Ended execution.", getClass().getName());
      clearDiagnosticContext();
    }
  }

  private void handleException(Tuple tuple, CommonTaskTuple commonTaskTuple, Exception e) {
    LOGGER.warn("{} error: {}", boltName(), e.getMessage(), e);
    if (commonTaskTuple != null) {
      var stack = new StringWriter();
      e.printStackTrace(new PrintWriter(stack));
      emitErrorNotification(tuple, commonTaskTuple, e.getMessage(), stack.toString());
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


  private void prepareDiagnosticContext(CommonTaskTuple commonTaskTuple) {
    DiagnosticContextWrapper.putValuesFrom(commonTaskTuple);
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
    declarer.declare(CommonTaskTuple.getFields());

    //notifications
    declarer.declareStream(NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
  }

  protected void emitErrorNotification(Tuple anchorTuple, CommonTaskTuple commonTaskTuple, String message, Throwable e) {
    emitErrorNotification(anchorTuple, commonTaskTuple, message,
        e.getMessage() + ":\n" + ExceptionUtils.getStackTrace(e));
  }

  protected void emitErrorNotification(Tuple anchorTuple, CommonTaskTuple commonTaskTuple, String message) {
    emitErrorNotification(anchorTuple, commonTaskTuple, message, (String) null);
  }

  protected void emitErrorNotification(Tuple anchorTuple, CommonTaskTuple commonTaskTuple, String message, String additionalInformation) {
    NotificationTuple nt = NotificationTuple.prepareNotificationWithResultResource(commonTaskTuple, RecordState.ERROR,
            message, additionalInformation);
    outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
  }

  protected void emitSuccessNotification(Tuple anchorTuple, CommonTaskTuple commonTaskTuple,
                                         String message, String additionalInformation,
                                         String unifiedErrorMessage, String detailedErrorMessage) {
    NotificationTuple nt = NotificationTuple.prepareNotificationWithResultResourceAndErrorMessage(commonTaskTuple, RecordState.SUCCESS, message, additionalInformation, unifiedErrorMessage, detailedErrorMessage);
    outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
  }

  protected void emitSuccessNotification(Tuple anchorTuple, CommonTaskTuple commonTaskTuple, String message, String additionalInformation) {
    NotificationTuple nt = NotificationTuple.prepareNotificationWithResultResource(commonTaskTuple, RecordState.SUCCESS, message, additionalInformation);
    outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, nt.toStormTuple());
  }

  protected void emitSuccessNotification(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {
    emitSuccessNotification(anchorTuple, commonTaskTuple, "", "");
  }

  protected void emitIgnoredNotification(Tuple anchorTuple, CommonTaskTuple commonTaskTuple,
                                         String message, String additionalInformation) {
    NotificationTuple tuple = NotificationTuple.prepareNotificationWithResultResource(commonTaskTuple, RecordState.SUCCESS, message, additionalInformation);
    tuple.addParameter(PluginParameterKeys.IGNORED_RECORD, "true");
    outputCollector.emit(NOTIFICATION_STREAM_NAME, anchorTuple, tuple.toStormTuple());
  }

  protected void prepareStormTaskTupleForEmission(CommonTaskTuple commonTaskTuple, String resultString)
          throws MalformedURLException {
    commonTaskTuple.setFileData(resultString.getBytes(StandardCharsets.UTF_8));
    final UrlParser urlParser = new UrlParser(commonTaskTuple.getRecordUri());
    commonTaskTuple.addParameter(PluginParameterKeys.CLOUD_ID, urlParser.getPart(UrlPart.RECORDS));
    commonTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_NAME, urlParser.getPart(UrlPart.REPRESENTATIONS));
    commonTaskTuple.addParameter(PluginParameterKeys.REPRESENTATION_VERSION, urlParser.getPart(UrlPart.VERSIONS));
  }

  protected void cleanInvalidData(CommonTaskTuple tuple) {
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
