package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;

/**
 * Will grant permissions to selected file for selected user
 * 
 */
public class GrantPermissionsToFileBolt extends AbstractDpsBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrantPermissionsToFileBolt.class);

    private RecordServiceClient recordServiceClient;
    private final String ecloudMcsAddress;
    private final String username;
    private final String password;

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
            String message = String.format("Empty fileUrl. Permissions will not be granted {}.");
            logAndEmitError(tuple, message);
            return;
        }
        if (submitterName == null) {
            String message = String.format("Empty submitter name. Permissions will not be granted");
            logAndEmitError(tuple, message);
            return;
        }

        LOGGER.info("Granting permissions for {} on {}", submitterName, resultFileUrl);
        grantPermissions(resultFileUrl, submitterName, tuple);
    }

    private String readResultFileUrl(StormTaskTuple tuple) {
        return tuple.getParameter(PluginParameterKeys.OUTPUT_URL);
    }

    private String readSubmitterName(StormTaskTuple tuple) {
        return tuple.getParameter(PluginParameterKeys.TASK_SUBMITTER_NAME);
    }

    void grantPermissions(String fileUrl, String userName, StormTaskTuple t) {
        try {
            UrlParser urlParser = new UrlParser(fileUrl);
            if (urlParser.isUrlToRepresentationVersionFile()) {
                recordServiceClient.grantPermissionsToVersion(
                        urlParser.getPart(UrlPart.RECORDS),
                        urlParser.getPart(UrlPart.REPRESENTATIONS),
                        urlParser.getPart(UrlPart.VERSIONS),
                        userName, Permission.ALL);
                emitSuccess(t);
            } else {
                String message = "Provided url does not point to ecloud file. Permissions will not be granted on: " + fileUrl;
                logAndEmitError(t, message);
            }

        } catch (MalformedURLException e) {
            String message = "Url to file is malformed. Permissions will not be granted on: " + fileUrl;
            logAndEmitError(t, message, e);
        } catch (MCSException e) {
            String message = "There was exception while trying to granted permissions on: " + fileUrl;
            logAndEmitError(t, message, e);
        }
    }

    private void logAndEmitError(StormTaskTuple t, String message) {
        LOGGER.error(message);
        emitErrorNotification(t.getTaskId(), t.getFileUrl(), message, t.getParameters().toString());
        emitBasicInfo(t.getTaskId(), 1);
    }

    private void logAndEmitError(StormTaskTuple t, String message, Exception e) {
        LOGGER.error(message, e);
        StringWriter stack = new StringWriter();
        e.printStackTrace(new PrintWriter(stack));
        logAndEmitError(t, message + e.getMessage());
    }
    
    private void emitSuccess(StormTaskTuple t){
        emitBasicInfo(t.getTaskId(), 1);
        outputCollector.emit(inputTuple, t.toStormTuple());
        outputCollector.ack(inputTuple);
    }
}
