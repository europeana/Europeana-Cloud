package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

import static eu.europeana.cloud.service.dps.storm.utils.Retriever.retryOnEcloudOnError;


/**
 * Read file/files from MCS and every file emits as separate {@link StormTaskTuple}.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ReadFileBolt extends AbstractDpsBolt {
    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadFileBolt.class);

    /**
     * Properties to connect to eCloud
     */
    private final String ecloudMcsAddress;
    protected transient FileServiceClient fileClient;

    public ReadFileBolt(String ecloudMcsAddress) {
        this.ecloudMcsAddress = ecloudMcsAddress;
    }

    @Override
    public void prepare() {
        fileClient = new FileServiceClient(ecloudMcsAddress);
    }

    @Override
    public void execute(Tuple anchorTuple, StormTaskTuple t) {
        final String file = t.getParameters().get(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER);
        try (InputStream is = getFileStreamByStormTuple(t)) {
            t.setFileData(is);
            outputCollector.emit(anchorTuple, t.toStormTuple());
        } catch (RepresentationNotExistsException | FileNotExistsException |
                WrongContentRangeException ex) {
            LOGGER.warn("Can not retrieve file at {}", file);
            emitErrorNotification(anchorTuple, t.getTaskId(), file, "Can not retrieve file", "The cause of the error is:" + ex.getCause(),
                    StormTaskTupleHelper.getRecordProcessingStartTime(t));
        } catch (Exception ex) {
            LOGGER.error("ReadFileBolt error: {}", ex.getMessage());
            emitErrorNotification(anchorTuple, t.getTaskId(), file, ex.getMessage(), "The cause of the error is:" + ex.getCause(),
                    StormTaskTupleHelper.getRecordProcessingStartTime(t));
        }
        outputCollector.ack(anchorTuple);
    }

    private InputStream getFile(FileServiceClient fileClient, String file, String authorization) throws Exception {
        return retryOnEcloudOnError("Error while getting a file", () ->
                fileClient.getFile(file, AUTHORIZATION, authorization));
    }

    protected InputStream getFileStreamByStormTuple(StormTaskTuple stormTaskTuple) throws Exception {
        final String file = stormTaskTuple.getParameters().get(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER);
        stormTaskTuple.setFileUrl(file);
        LOGGER.info("HERE THE LINK: {}", file);
        return getFile(fileClient, file, stormTaskTuple.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER));
    }

}
