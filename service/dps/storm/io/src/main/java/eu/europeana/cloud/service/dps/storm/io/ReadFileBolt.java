package eu.europeana.cloud.service.dps.storm.io;

import com.rits.cloning.Cloner;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;


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
        Map<String, String> parameters = t.getParameters();
        List<String> files = Arrays.asList(parameters.get(PluginParameterKeys.DPS_TASK_INPUT_DATA).split("\\s*,\\s*"));
        if (files != null && !files.isEmpty()) {
            t.getParameters().remove(PluginParameterKeys.DPS_TASK_INPUT_DATA);
            emitFiles(t, files);
            return;
        } else {
            String errorMessage = "No URL for retrieve file.";
            LOGGER.warn(errorMessage);
            emitErrorNotification(t.getTaskId(), "", errorMessage, parameters.toString());
        }
    }

    private void emitFiles(StormTaskTuple t, List<String> files) {
        StormTaskTuple tt;
        final String authorizationHeader = t.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        FileServiceClient fileClient = new FileServiceClient(ecloudMcsAddress);
        fileClient.useAuthorizationHeader(authorizationHeader);
        for (String file : files) {
            tt = new Cloner().deepClone(t);  //without cloning every emitted tuple will have the same object!!!
            try {
                LOGGER.info("HERE THE LINK: " + file);
                InputStream is = fileClient.getFile(file);
                tt.setFileData(is);
                tt.setFileUrl(file);
                outputCollector.emit(inputTuple, tt.toStormTuple());
            } catch (RepresentationNotExistsException | FileNotExistsException |
                    WrongContentRangeException ex) {
                LOGGER.warn("Can not retrieve file at {}", file);
                emitErrorNotification(t.getTaskId(), file, "Can not retrieve file", "");
            } catch (DriverException | MCSException | IOException ex) {
                LOGGER.error("ReadFileBolt error:" + ex.getMessage());
                emitErrorNotification(t.getTaskId(), file, ex.getMessage(), t.getParameters().toString());
            }
        }
    }
}
