package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.Date;

/**
 * Adds defined revisions to given representationVersion
 */
public class RevisionWriterBolt extends AbstractDpsBolt {

    public static final Logger LOGGER = LoggerFactory.getLogger(RevisionWriterBolt.class);

    private String ecloudMcsAddress;

    public RevisionWriterBolt(String ecloudMcsAddress) {
        this.ecloudMcsAddress = ecloudMcsAddress;
    }

    @Override
    public void execute(StormTaskTuple stormTaskTuple) {
        RevisionServiceClient revisionsClient = new RevisionServiceClient(ecloudMcsAddress);
        final String authorizationHeader = stormTaskTuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        revisionsClient.useAuthorizationHeader(authorizationHeader);
        addRevisionAndEmit(stormTaskTuple, revisionsClient);
    }

    protected void addRevisionAndEmit(StormTaskTuple stormTaskTuple, RevisionServiceClient revisionsClient) {
        LOGGER.info("{} executed", getClass().getSimpleName());
        try {
            addRevisionToSpecificResource(stormTaskTuple, revisionsClient, stormTaskTuple.getParameter(PluginParameterKeys.OUTPUT_URL));
            outputCollector.emit(stormTaskTuple.toStormTuple());
        } catch (MalformedURLException e) {
            LOGGER.error("URL is malformed: {} ", stormTaskTuple.getParameter(PluginParameterKeys.OUTPUT_URL));
            emitErrorNotification(stormTaskTuple.getTaskId(), null, e.getMessage(), stormTaskTuple.getParameters().toString());
        } catch (MCSException e) {
            LOGGER.warn("Error while communicating with MCS {}", e.getMessage());
            emitErrorNotification(stormTaskTuple.getTaskId(), null, e.getMessage(), stormTaskTuple.getParameters().toString());
        }
    }

    protected void addRevisionToSpecificResource(StormTaskTuple stormTaskTuple, RevisionServiceClient revisionsClient, String affectedResourceURL) throws MalformedURLException, MCSException {
        if (stormTaskTuple.hasRevisionToBeApplied()) {
            LOGGER.info("Adding revisions to representation version: {}", stormTaskTuple.getFileUrl());
            final UrlParser urlParser = new UrlParser(affectedResourceURL);
            Revision revisionToBeApplied = stormTaskTuple.getRevisionToBeApplied();
            if (revisionToBeApplied.getCreationTimeStamp() == null)
                revisionToBeApplied.setCreationTimeStamp(new Date());
            revisionsClient.addRevision(
                    urlParser.getPart(UrlPart.RECORDS),
                    urlParser.getPart(UrlPart.REPRESENTATIONS),
                    urlParser.getPart(UrlPart.VERSIONS),
                    revisionToBeApplied);
            stormTaskTuple.setRevisionToBeApplied(null);
        } else {
            LOGGER.info("Revisions list is empty");
        }
    }

    @Override
    public void prepare() {

    }
}
