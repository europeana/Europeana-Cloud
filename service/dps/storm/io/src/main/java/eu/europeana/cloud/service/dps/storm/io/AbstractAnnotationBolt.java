package eu.europeana.cloud.service.dps.storm.io;


import static eu.europeana.cloud.service.dps.Constants.DPS_REPRESENTATION_NAME;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.CLOUD_ID;

import eu.europeana.cloud.common.model.AnnotationsDto;
import eu.europeana.cloud.common.model.RepresentationVersionAnnotation;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores a Record on the cloud.
 * <p/>
 * Receives a byte array representing a Record from a tuple, creates and stores a new Record on the cloud, and emits the URL of
 * the newly created record.
 */
public abstract class AbstractAnnotationBolt extends AbstractDpsBolt {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAnnotationBolt.class);
  private final String ecloudMcsAddress;
  private final String topologyUserName;
  private final String topologyUserPassword;
  protected transient RecordServiceClient recordServiceClient;

  public AbstractAnnotationBolt(CassandraProperties cassandraProperties, String ecloudMcsAddress, String topologyUserName,
      String topologyUserPassword) {
    super(cassandraProperties);
    this.ecloudMcsAddress = ecloudMcsAddress;
    this.topologyUserName = topologyUserName;
    this.topologyUserPassword = topologyUserPassword;
  }

  @Override
  protected boolean ignoreDeleted() {
    return false;
  }

  @Override
  public void prepare() {
    LOGGER.debug("Preparing MCS client with the following params url={} user={}", ecloudMcsAddress, topologyUserName);
    recordServiceClient = new RecordServiceClient(ecloudMcsAddress, topologyUserName, topologyUserPassword);
  }

  @Override
  public void execute(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {
    LOGGER.debug("AbstractAnnotationBolt: adding annotation to record: {}",commonTaskTuple.getRecordUri());
    try {
      String cloudId = commonTaskTuple.getParameter(CLOUD_ID);
      String version = commonTaskTuple.getParameter(PluginParameterKeys.REPRESENTATION_VERSION);
      recordServiceClient.addAnnotationToRepresentationVersion(
          cloudId,DPS_REPRESENTATION_NAME,version,new AnnotationsDto(createAnnotation(commonTaskTuple)));

      emitSuccessfulResult(anchorTuple, commonTaskTuple);
    } catch (RetryInterruptedException e) {
      handleInterruption(e, anchorTuple);
    } catch (Exception e) {
      LOGGER.warn("Unable to process the message", e);
      StringWriter stack = new StringWriter();
      e.printStackTrace(new PrintWriter(stack));
        emitErrorNotification(anchorTuple, commonTaskTuple, "Cannot process data because: " + e.getMessage(), stack.toString());
        outputCollector.ack(anchorTuple);
    }
  }

  protected abstract RepresentationVersionAnnotation createAnnotation(CommonTaskTuple tuple);

  protected void emitSuccessfulResult(Tuple anchorTuple, CommonTaskTuple commonTaskTuple) {
    emitNotification(anchorTuple, commonTaskTuple);
  }

}

