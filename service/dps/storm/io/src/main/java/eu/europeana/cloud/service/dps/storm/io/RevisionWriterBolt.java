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
import java.net.URI;

/**
 * Adds defined revisions to given representationVersion
 *
 */
public class RevisionWriterBolt extends AbstractDpsBolt {

	public static final Logger LOGGER = LoggerFactory.getLogger(RevisionWriterBolt.class);

	private String ecloudMcsAddress;

	private RevisionServiceClient revisionsClient;

	public RevisionWriterBolt(String ecloudMcsAddress) {
		this.ecloudMcsAddress = ecloudMcsAddress;
	}

	@Override
	public void execute(StormTaskTuple stormTaskTuple) {
		LOGGER.info(getClass().getSimpleName() + " executed");
		try {
			if (stormTaskTuple.hasRevisionsToBeApplied()) {
				LOGGER.info("Adding revisions to representation version: " + stormTaskTuple.getFileUrl());
				addDefinedRevisions(stormTaskTuple);
			} else {
				LOGGER.info("Revisions list is empty");
			}
		} catch (MalformedURLException e) {
			e.printStackTrace();
			emitErrorNotification(stormTaskTuple.getTaskId(), null, e.getMessage(), stormTaskTuple.getParameters().toString());
		} catch (MCSException e) {
			e.printStackTrace();
			LOGGER.warn("Error while communicating with MCS", e.getMessage());
			emitErrorNotification(stormTaskTuple.getTaskId(), null, e.getMessage(), stormTaskTuple.getParameters().toString());
		}
		outputCollector.emit(inputTuple, stormTaskTuple.toStormTuple());
	}

	@Override
	public void prepare() {
		revisionsClient = new RevisionServiceClient(ecloudMcsAddress);
	}

	protected void addDefinedRevisions(StormTaskTuple stormTaskTuple) throws MalformedURLException, MCSException {
		final UrlParser urlParser = new UrlParser(stormTaskTuple.getFileUrl().toString());
		revisionsClient.useAuthorizationHeader(stormTaskTuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER));
		for (Revision revisionToBeApplied : stormTaskTuple.getRevisionsToBeApplied()) {
			revisionsClient.addRevision(
					urlParser.getPart(UrlPart.RECORDS),
					urlParser.getPart(UrlPart.REPRESENTATIONS),
					urlParser.getPart(UrlPart.VERSIONS),
					revisionToBeApplied);
		}
	}
}
