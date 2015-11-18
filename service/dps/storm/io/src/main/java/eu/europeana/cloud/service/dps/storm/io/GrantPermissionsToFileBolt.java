package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;

/**
 * Will grant permissions to selected file for selected user
 * 
 */
public class GrantPermissionsToFileBolt extends AbstractDpsBolt {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(GrantPermissionsToFileBolt.class);

    private RecordServiceClient recordServiceClient;
    private UrlParser urlParser;
    private String ecloudMcsAddress;
    private String username;
    private String password;

    public GrantPermissionsToFileBolt(String ecloudMcsAddress, String username, String password) {
        this.ecloudMcsAddress = ecloudMcsAddress;
        this.username = username;
        this.password = password;
    }

    @Override
    public void prepare() {
        recordServiceClient = new RecordServiceClient(ecloudMcsAddress, username, password);
    }

    @Override
    public void execute(StormTaskTuple tuple) {
        String submitterName = readSubmitterName(tuple);
        String resultFileUrl = readResultFileUrl(tuple);
        if (resultFileUrl == null) {
            LOGGER.info("Empty fileUrl. Permissions will not be granted");
            outputCollector.ack(inputTuple);
            return;
        }
        if (submitterName == null) {
            LOGGER.info("Empty submitter name. Permissions will not be granted");
            outputCollector.ack(inputTuple);
            return;
        }

        LOGGER.info("Granting permissions for {} on {}", submitterName, resultFileUrl);
        grantPermissions(resultFileUrl, submitterName);
        emitBasicInfo(tuple.getTaskId(), 1);
        outputCollector.emit(inputTuple, tuple.toStormTuple());
        outputCollector.ack(inputTuple);
    }

    private String readResultFileUrl(StormTaskTuple tuple) {
        return tuple.getParameter(PluginParameterKeys.OUTPUT_URL);
    }

    private String readSubmitterName(StormTaskTuple tuple) {
        return tuple.getParameter(PluginParameterKeys.TASK_SUBMITTER_NAME);
    }

    public void grantPermissions(String fileUrl, String userName) {
        try {
            urlParser = new UrlParser(fileUrl);
            if (urlParser.isUrlToRepresentationVersionFile()) {
                recordServiceClient.grantPermissionsToVersion(
                        urlParser.getPart(UrlPart.RECORDS),
                        urlParser.getPart(UrlPart.REPRESENTATIONS),
                        urlParser.getPart(UrlPart.VERSIONS),
                        userName, Permission.ALL);
            } else {
                LOGGER.error("Provided url does not point to ecloud file. Permissions will not be granted on: {}", fileUrl);
            }

        } catch (MalformedURLException e) {
            LOGGER.error("Url to file is malformed. Permissions will not be granted on: {}", fileUrl);
        } catch (MCSException e) {
            LOGGER.error("There was exception while trying to granted permissions on: {}", fileUrl);
            e.printStackTrace();
        }
    }
}
