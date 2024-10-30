package eu.europeana.cloud.harvesting;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.tuple.Tuple;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.List;

/**
 * Bolt that will check if there are duplicates in harvested records.</br> Duplicates, in this context, are representation
 * versions that have the same cloud_id, representation name and revision</br>
 * <p>In case of duplicate detection current representation version is removed from the system and error is reported.</p>
 */
public class DuplicatedRecordsProcessorBolt extends AbstractDpsBolt {

  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(DuplicatedRecordsProcessorBolt.class);
  private transient RecordServiceClient recordServiceClient;
  private transient RevisionServiceClient revisionServiceClient;
  private final String ecloudMcsAddress;
  private final String ecloudMcsUser;
  private final String ecloudMcsUserPassword;

  /**
   * Constructs instance of the {@link DuplicatedRecordsProcessorBolt}
   *
   * @param cassandraProperties Cassandra cluster addresses and credentials.
   * @param ecloudMcsAddress location of the MCS
   * @param ecloudMcsUser username required for communication with MCS
   * @param ecloudMcsUserPassword user password required for communication with MCS
   */
  public DuplicatedRecordsProcessorBolt(CassandraProperties cassandraProperties,
      String ecloudMcsAddress, String ecloudMcsUser, String ecloudMcsUserPassword) {
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
  public void prepare() {
    recordServiceClient = new RecordServiceClient(ecloudMcsAddress, ecloudMcsUser, ecloudMcsUserPassword);
    revisionServiceClient = new RevisionServiceClient(ecloudMcsAddress, ecloudMcsUser, ecloudMcsUserPassword);
  }

  @Override
  public void execute(Tuple anchorTuple, StormTaskTuple tuple) {
    LOGGER.info("Checking duplicates for oai identifier '{}' and task '{}'", tuple.getFileUrl(), tuple.getTaskId());
    try {
      Representation representation = extractRepresentationInfoFromTuple(tuple);
      List<RepresentationRevisionResponse> representationRevisions = findRepresentationsWithSameRevision(tuple, representation);
      if (representationsWithSameRevisionExists(representationRevisions)) {
        handleDuplicatedRepresentation(anchorTuple, tuple, representation);
        return;
      }
      emitSuccessNotification(anchorTuple, tuple, "", "");
        LOGGER.info("Checking duplicates finished for oai identifier '{}' nad task '{}'", tuple.getFileUrl(), tuple.getTaskId());
    } catch (MalformedURLException | MCSException e) {
        LOGGER.error("Error while detecting duplicates", e);
        emitErrorNotification(
                anchorTuple,
                tuple,
                "Error while detecting duplicates",
                e.getMessage());
    }
    outputCollector.ack(anchorTuple);
  }

  private void handleDuplicatedRepresentation(Tuple anchorTuple, StormTaskTuple tuple, Representation representation)
      throws MCSException {
    LOGGER.warn("Found same revision for '{}' and '{}'", tuple.getFileUrl(), tuple.getTaskId());
    removeRevision(tuple, representation);
    removeRepresentation(representation);
    emitErrorNotification(
        anchorTuple,
        tuple,
        "Duplicate detected",
        "Duplicate detected for " + tuple.getFileUrl());
    outputCollector.ack(anchorTuple);
  }

  private void removeRepresentation(Representation representation) throws MCSException {
    recordServiceClient.deleteRepresentation(
        representation.getCloudId(),
        representation.getRepresentationName(),
        representation.getVersion());
  }

  private void removeRevision(StormTaskTuple tuple, Representation representation) throws MCSException {
    revisionServiceClient.deleteRevision(
        representation.getCloudId(),
        representation.getRepresentationName(),
        representation.getVersion(),
        tuple.getRevisionToBeApplied());
  }

  private List<RepresentationRevisionResponse> findRepresentationsWithSameRevision(StormTaskTuple tuple, Representation representation)
      throws MCSException {
    return recordServiceClient.getRepresentationRawRevisions(
        representation.getCloudId(), representation.getRepresentationName(),
        new Revision(
            tuple.getRevisionToBeApplied().getRevisionName(),
            tuple.getRevisionToBeApplied().getRevisionProviderId(),
            //TODO there is helper class for that
            new DateTime(tuple.getRevisionToBeApplied().getCreationTimeStamp(), DateTimeZone.UTC).toDate())
    );
  }

  private boolean representationsWithSameRevisionExists(List<RepresentationRevisionResponse> representationRevisions) {
    return representationRevisions.size() > 1;
  }

  private Representation extractRepresentationInfoFromTuple(StormTaskTuple tuple) throws MalformedURLException, MCSException {
    Representation representation = new Representation();
    UrlParser parser = new UrlParser(tuple.getParameters().get(PluginParameterKeys.OUTPUT_URL));
    if (parser.isUrlToRepresentationVersion() || parser.isUrlToRepresentationVersionFile()) {
      representation.setCloudId(parser.getPart(UrlPart.RECORDS));
      representation.setRepresentationName(parser.getPart(UrlPart.REPRESENTATIONS));
      representation.setVersion(parser.getPart(UrlPart.VERSIONS));
      return representation;
    }
    throw new MCSException("Output URL is not URL to the representation version file");
  }

  @Override
  protected void cleanInvalidData(StormTaskTuple tuple) {
    int attemptNumber = tuple.getRecordAttemptNumber();
    LOGGER.error("Attempt number {} to process this message. No cleaning needed here.", attemptNumber);
    // nothing to clean here when the message is reprocessed
  }
}
