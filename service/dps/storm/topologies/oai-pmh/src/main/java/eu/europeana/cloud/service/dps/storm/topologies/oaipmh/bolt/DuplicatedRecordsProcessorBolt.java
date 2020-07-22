package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import eu.europeana.cloud.common.model.Representation;
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
 * Bolt that will check if there are duplicates in harvested records.</br>
 * Duplicates, in this context, are representation versions that have the same cloud_id, representation name and revision</br>
 *
 */
public class DuplicatedRecordsProcessorBolt extends AbstractDpsBolt {

    protected static final String AUTHORIZATION = "Authorization";
    private static final Logger logger = LoggerFactory.getLogger(DuplicatedRecordsProcessorBolt.class);
    private transient RecordServiceClient recordServiceClient;
    private transient RevisionServiceClient revisionServiceClient;
    private final String ecloudMcsAddress;

    public DuplicatedRecordsProcessorBolt(String ecloudMcsAddress) {
        this.ecloudMcsAddress = ecloudMcsAddress;
    }

    @Override
    public void prepare() {
        recordServiceClient = new RecordServiceClient(ecloudMcsAddress);
        revisionServiceClient = new RevisionServiceClient(ecloudMcsAddress);
    }

    @Override
    public void execute(Tuple anchorTuple, StormTaskTuple tuple) {
        logger.info("Checking duplicates for oai identifier '{}' nad task '{}'", tuple.getFileUrl(), tuple.getTaskId());
        try {
            Representation representation = extractRepresentationInfoFromTuple(tuple);
            List<Representation> representations = findRepresentationsWithSameRevision(tuple, representation);
            if (representationsWithSameRevisionExists(representations)) {
                handleDuplicatedRepresentation(anchorTuple, tuple, representation);
                return;
            }
            logger.info("Checking duplicates finished for oai identifier '{}' nad task '{}'", tuple.getFileUrl(), tuple.getTaskId());
            outputCollector.emit(anchorTuple, tuple.toStormTuple());
            outputCollector.ack(anchorTuple);
        } catch (MalformedURLException | MCSException e) {
            logger.error("Error while detecting duplicates");
            emitErrorNotification(
                    anchorTuple,
                    tuple.getTaskId(),
                    tuple.getFileUrl(),
                    "Error while detecting duplicates",
                    e.getMessage());
        }
    }

    private void handleDuplicatedRepresentation(Tuple anchorTuple, StormTaskTuple tuple, Representation representation) throws MCSException {
        logger.warn("Found same revision for '{}' and '{}'", tuple.getFileUrl(), tuple.getTaskId());
        removeRevision(tuple, representation);
        removeRepresentation(tuple, representation);
        emitErrorNotification(
                anchorTuple,
                tuple.getTaskId(),
                tuple.getFileUrl(),
                "Duplicate detected",
                "Duplicate detected for " + tuple.getFileUrl());
    }

    private void removeRepresentation(StormTaskTuple tuple, Representation representation) throws MCSException {
        recordServiceClient.deleteRepresentation(
                representation.getCloudId(),
                representation.getRepresentationName(),
                representation.getVersion(),
                AUTHORIZATION, tuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER));
    }

    private void removeRevision(StormTaskTuple tuple, Representation representation) throws MCSException {
        revisionServiceClient.deleteRevision(
                representation.getCloudId(),
                representation.getRepresentationName(),
                representation.getVersion(),
                tuple.getRevisionToBeApplied().getRevisionName(),
                tuple.getRevisionToBeApplied().getRevisionProviderId(),
                new DateTime(tuple.getRevisionToBeApplied().getCreationTimeStamp(), DateTimeZone.UTC).toString(),
                AUTHORIZATION, tuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER));
    }

    private List<Representation> findRepresentationsWithSameRevision(StormTaskTuple tuple, Representation representation) throws MCSException {
        return recordServiceClient.getRepresentationsByRevision(
                representation.getCloudId(), representation.getRepresentationName(),
                tuple.getRevisionToBeApplied().getRevisionName(),
                tuple.getRevisionToBeApplied().getRevisionProviderId(),
                new DateTime(tuple.getRevisionToBeApplied().getCreationTimeStamp(), DateTimeZone.UTC).toString(),
                AUTHORIZATION,
                tuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER));
    }

    private boolean representationsWithSameRevisionExists(List<Representation> representations) {
        return representations.size() > 1;
    }

    private Representation extractRepresentationInfoFromTuple(StormTaskTuple tuple) throws MalformedURLException, MCSException {
        Representation representation = new Representation();
        UrlParser parser = new UrlParser(tuple.getParameters().get(PluginParameterKeys.OUTPUT_URL));
        if (parser.isUrlToRepresentationVersionFile()) {
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
        logger.error("Attempt number {} to process this message. No cleaning needed here.", attemptNumber);
        // nothing to clean here when the message is reprocessed
    }
}
