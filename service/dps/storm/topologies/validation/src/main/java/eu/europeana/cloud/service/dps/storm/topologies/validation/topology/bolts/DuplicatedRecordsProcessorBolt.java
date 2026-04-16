package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

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
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.tuple.Tuple;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.List;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.DUPLICATED_RECORD;

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
  public void execute(Tuple anchorTuple, CommonTaskTuple tuple) {
    LOGGER.info("Checking duplicates for record url '{}' and task '{}'", tuple.getRecordUri(), tuple.getTaskId());
      try {
        Representation representation = extractRepresentationInfoFromTuple(tuple);
      Revision revision = tuple.getOutputRevision();
        // Based on processing mode we either look for representation with same revisions or representations with same versions
        if (revision != null) {
          if (detectAndHandleDuplicatesInRevisionBasedProcessing(anchorTuple, tuple, representation, revision)) {
            return;
          }
        } else {
          if (detectAndHandleDuplicatesInRepresentationBasedProcessing(anchorTuple, tuple, representation)) {
            return;
          }
        }
        emitSuccessNotification(anchorTuple, tuple, "", "");
        LOGGER.info("Checking duplicates finished for record url '{}' and task '{}'", tuple.getRecordUri(), tuple.getTaskId());
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

  private boolean detectAndHandleDuplicatesInRepresentationBasedProcessing(Tuple anchorTuple, CommonTaskTuple tuple,
                                                                           Representation representation) throws MCSException {
    List<Representation> representations = findAllRepresentationWithSameCloudId(representation);
    if (representationsWithSameCloudIdExist(representations)) {
      tuple.addParameter(DUPLICATED_RECORD, "true");
      handleDuplicatedRepresentationVersion(anchorTuple, tuple, representation);
      return true;
    }
    return false;
  }

  private List<Representation> findAllRepresentationWithSameCloudId(Representation representation) throws MCSException {
    return recordServiceClient
            .getRepresentations(representation.getCloudId(), representation.getRepresentationName())
            .stream().filter(rep -> representation.getDatasetId().equals(rep.getDatasetId()) &&
                    representation.getDataProvider().equals(rep.getDataProvider())).toList();
  }

  private boolean detectAndHandleDuplicatesInRevisionBasedProcessing(Tuple anchorTuple, CommonTaskTuple tuple,
                                                                     Representation representation, Revision revision) throws MCSException {
    List<RepresentationRevisionResponse> representationRevisions = findRepresentationsWithSameRevision(representation, revision);
    if (representationsWithSameRevisionExists(representationRevisions)) {
      tuple.addParameter(DUPLICATED_RECORD, "true");
      handleDuplicatedRepresentationRevision(anchorTuple, tuple, representation);
      return true;
    }
    return false;
  }

  private boolean representationsWithSameCloudIdExist(List<Representation> representationsAlreadyExisting) {
    return representationsAlreadyExisting.size() > 1;
  }

  private void handleDuplicatedRepresentationRevision(Tuple anchorTuple, CommonTaskTuple tuple, Representation representation)
          throws MCSException {
      LOGGER.warn("Found same revision for '{}' and '{}'", tuple.getRecordUri(), tuple.getTaskId());
      removeRevision(tuple, representation);
      removeRepresentation(representation);
      emitErrorNotification(
              anchorTuple,
              tuple,
              "Duplicate detected",
              "Duplicate detected for " + tuple.getRecordUri());
      outputCollector.ack(anchorTuple);
  }

  private void handleDuplicatedRepresentationVersion(Tuple anchorTuple, CommonTaskTuple tuple, Representation representation)
          throws MCSException {
      LOGGER.warn("Found same version for '{}' and '{}'", tuple.getRecordUri(), tuple.getTaskId());
      removeRepresentation(representation);
      emitErrorNotification(
              anchorTuple,
              tuple,
              "Duplicate detected",
              "Duplicate detected for " + tuple.getRecordUri());
      outputCollector.ack(anchorTuple);
  }

  private void removeRepresentation(Representation representation) throws MCSException {
    recordServiceClient.deleteRepresentation(
            representation.getCloudId(),
            representation.getRepresentationName(),
            representation.getVersion());
  }

  private void removeRevision(CommonTaskTuple tuple, Representation representation) throws MCSException {
    revisionServiceClient.deleteRevision(
            representation.getCloudId(),
            representation.getRepresentationName(),
            representation.getVersion(),
            tuple.getOutputRevision());
  }

  private List<RepresentationRevisionResponse> findRepresentationsWithSameRevision(Representation representation, Revision outputRevision)
          throws MCSException {
    return recordServiceClient.getRepresentationRawRevisions(
            representation.getCloudId(), representation.getRepresentationName(),
            new Revision(
                    outputRevision.getRevisionName(),
                    outputRevision.getRevisionProviderId(),
                    //TODO there is helper class for that
                    new DateTime(outputRevision.getCreationTimeStamp(), DateTimeZone.UTC).toDate())
    );
  }

  private boolean representationsWithSameRevisionExists(List<RepresentationRevisionResponse> representationRevisions) {
    return representationRevisions.size() > 1;
  }

  private Representation extractRepresentationInfoFromTuple(CommonTaskTuple tuple) throws MalformedURLException, MCSException {
    Representation representation = new Representation();
    // If new representation was added in writeRecordBolt and put into OUTPUT_URL then use it,
    // otherwise go with fileUrl
    String uri = (tuple.ifParametersContainsKey(PluginParameterKeys.OUTPUT_URL) ? tuple.getParameter(PluginParameterKeys.OUTPUT_URL) : tuple.getRecordUri());
    UrlParser parser = new UrlParser(uri);
    if (parser.isUrlToRepresentationVersion() || parser.isUrlToRepresentationVersionFile()) {
      representation.setCloudId(parser.getPart(UrlPart.RECORDS));
      representation.setRepresentationName(parser.getPart(UrlPart.REPRESENTATIONS));
      representation.setVersion(parser.getPart(UrlPart.VERSIONS));
    } else {
      throw new MCSException("Output URL is not URL to the representation version file");
    }
    if (tuple.getOutputDatasetId() != null) {
      representation.setDatasetId(tuple.getOutputDatasetId());
      representation.setDataProvider(tuple.getOutputDatasetProvider());
    }

    return representation;
  }

  @Override
  protected void cleanInvalidData(CommonTaskTuple tuple) {
    int attemptNumber = tuple.getRecordAttemptNumber();
    LOGGER.error("Attempt number {} to process this message. No cleaning needed here.", attemptNumber);
    // nothing to clean here when the message is reprocessed
  }
}
