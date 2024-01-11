package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.common.utils.Clock;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.time.Instant;
import java.util.Date;

/**
 * Adds defined revisions to given representationVersion. This is default implementation where simply 'SuccessNotification' is
 * emitted at the end to the {@link eu.europeana.cloud.service.dps.storm.NotificationBolt}.
 */
public class RevisionWriterBolt extends AbstractDpsBolt {

  private static final long serialVersionUID = 1L;
  public static final Logger LOGGER = LoggerFactory.getLogger(RevisionWriterBolt.class);

  protected transient RevisionServiceClient revisionsClient;

  private final String ecloudMcsAddress;
  private final String ecloudMcsUser;
  private final String ecloudMcsUserPassword;

  public RevisionWriterBolt(CassandraProperties cassandraProperties, String ecloudMcsAddress,
      String ecloudMcsUser, String ecloudMcsUserPassword) {
    super(cassandraProperties);
    this.ecloudMcsAddress = ecloudMcsAddress;
    this.ecloudMcsUser = ecloudMcsUser;
    this.ecloudMcsUserPassword = ecloudMcsUserPassword;
  }

  @Override
  protected boolean ignoreDeleted() {
    return false;
  }

  @Override
  public void execute(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    addRevisionAndEmit(anchorTuple, stormTaskTuple);
    outputCollector.ack(anchorTuple);
  }

  protected void addRevisionAndEmit(Tuple anchorTuple, StormTaskTuple stormTaskTuple) {
    LOGGER.info("Adding revision to the file");
    Instant processingStartTime = Instant.now();
    String resourceURL = getResourceUrl(stormTaskTuple);
    try {
      addRevisionToSpecificResource(stormTaskTuple, resourceURL);
      emitTuple(anchorTuple, stormTaskTuple);

    } catch (MalformedURLException e) {
      LOGGER.error("URL is malformed: {} ", resourceURL);
        emitErrorNotification(anchorTuple, stormTaskTuple, e.getMessage(), "The cause of the error is:" + e.getCause());
    } catch (MCSException | DriverException e) {
        LOGGER.warn("Error while communicating with MCS {}", e.getMessage());
        emitErrorNotification(anchorTuple, stormTaskTuple, e.getMessage(), "The cause of the error is:" + e.getCause());
    }
    LOGGER.info("Revision added in: {}ms", Clock.millisecondsSince(processingStartTime));
  }

  protected void emitTuple(Tuple anchorTuple, StormTaskTuple tuple) {
    if (tupleContainsErrors(tuple)) {
      emitSuccessNotificationContainingErrorInfo(anchorTuple, tuple);
    } else {
        emitSuccessNotification(anchorTuple, tuple, "", "");
    }
  }

  private boolean tupleContainsErrors(StormTaskTuple tuple) {
    return tuple.getParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE) != null;
  }

  private void emitSuccessNotificationContainingErrorInfo(Tuple anchorTuple, StormTaskTuple tuple) {
      emitSuccessNotification(anchorTuple, tuple, "", "",
              tuple.getParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE),
              tuple.getParameter(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE));
  }

  private String getResourceUrl(StormTaskTuple stormTaskTuple) {
    String resourceURL = stormTaskTuple.getParameter(PluginParameterKeys.OUTPUT_URL);
    if (resourceURL == null) {
      resourceURL = stormTaskTuple.getFileUrl();
    }
    return resourceURL;
  }

  protected void addRevisionToSpecificResource(StormTaskTuple stormTaskTuple, String affectedResourceURL)
      throws MalformedURLException, MCSException {
    if (stormTaskTuple.hasRevisionToBeApplied()) {
      LOGGER.info("The following revision will be added: {}", stormTaskTuple.getRevisionToBeApplied());
      final UrlParser urlParser = new UrlParser(affectedResourceURL);
      Revision revisionToBeApplied = stormTaskTuple.getRevisionToBeApplied();
      if (revisionToBeApplied.getCreationTimeStamp() == null) {
        revisionToBeApplied.setCreationTimeStamp(new Date());
      }

      if (stormTaskTuple.isMarkedAsDeleted()) {
        revisionToBeApplied = new Revision(revisionToBeApplied);
        revisionToBeApplied.setDeleted(true);
      }

      addRevision(urlParser, revisionToBeApplied);
    } else {
      LOGGER.info("Revisions list is empty");
    }
  }

  private void addRevision(UrlParser urlParser, Revision revisionToBeApplied) throws MCSException {
    RetryableMethodExecutor.executeOnRest("Error while adding Revisions", () ->
        revisionsClient.addRevision(
            urlParser.getPart(UrlPart.RECORDS),
            urlParser.getPart(UrlPart.REPRESENTATIONS),
            urlParser.getPart(UrlPart.VERSIONS),
            revisionToBeApplied)
    );
  }

  @Override
  public void prepare() {
    if (ecloudMcsAddress == null) {
      throw new NullPointerException("MCS Server must be set!");
    }
    revisionsClient = new RevisionServiceClient(ecloudMcsAddress, ecloudMcsUser, ecloudMcsUserPassword);
  }

  @Override
  protected void cleanInvalidData(StormTaskTuple tuple) {
    int attemptNumber = tuple.getRecordAttemptNumber();
    LOGGER.info("Attempt number {} to process this message. No cleaning needed here.", attemptNumber);
    // nothing to clean here when the message is reprocessed
  }
}
