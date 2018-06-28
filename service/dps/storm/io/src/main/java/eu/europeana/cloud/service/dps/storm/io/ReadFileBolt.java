package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;


/**
 * Read file/files from MCS and every file emits as separate {@link StormTaskTuple}.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ReadFileBolt extends AbstractDpsBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadFileBolt.class);

    /**
     * Properties to connect to eCloud
     */
    private final String ecloudMcsAddress;

    public ReadFileBolt(String ecloudMcsAddress) {
        this.ecloudMcsAddress = ecloudMcsAddress;
    }

    @Override
    public void prepare() {
    }

    @Override
    public void execute(StormTaskTuple t) {
        String file = t.getParameters().get(PluginParameterKeys.DPS_TASK_INPUT_DATA);
        FileServiceClient fileClient = new FileServiceClient(ecloudMcsAddress);
        try {
            final String authorizationHeader = t.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
            fileClient.useAuthorizationHeader(authorizationHeader);
            t.getParameters().remove(PluginParameterKeys.DPS_TASK_INPUT_DATA);
            emitFile(t, fileClient, file);
        } finally {
            fileClient.close();
        }
    }

    void emitFile(StormTaskTuple t, FileServiceClient fileClient, String file) {

        LOGGER.info("HERE THE LINK: {}", file);
        try (InputStream is = getFile(fileClient, file)) {
            t.setFileData(is);
            t.setFileUrl(file);
            outputCollector.emit(t.toStormTuple());
        } catch (RepresentationNotExistsException | FileNotExistsException |
                WrongContentRangeException ex) {
            LOGGER.warn("Can not retrieve file at {}", file);
            emitErrorNotification(t.getTaskId(), file, "Can not retrieve file", "");
        } catch (DriverException | MCSException | IOException ex) {
            LOGGER.error("ReadFileBolt error: {}" , ex.getMessage());
            emitErrorNotification(t.getTaskId(), file, ex.getMessage(), t.getParameters().toString());
        }
    }


    private InputStream getFile(FileServiceClient fileClient, String file) throws MCSException, IOException, DriverException {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                return fileClient.getFile(file);
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while getting a file. Retries left:{} ", retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while getting a file.");
                    throw e;
                }
            }
        }
    }
}
