package eu.europeana.cloud.service.dps.storm.io;

import backtype.storm.task.OutputCollector;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rits.cloning.Cloner;

/**
 * Read dataset and emit every file in dataset as a separate {@link StormTaskTuple}.
 */
public class ReadDatasetBolt extends AbstractDpsBolt {
    private final String ecloudMcsAddress;
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadDatasetBolt.class);
    private DataSetServiceClient datasetClient;
    private FileServiceClient fileClient;

    /**
     * Constructor of ReadDatasetBolt.
     *
     * @param ecloudMcsAddress MCS API URL
     */
    public ReadDatasetBolt(String ecloudMcsAddress) {
        this.ecloudMcsAddress = ecloudMcsAddress;

    }

    /**
     * Should be used only on tests.
     */
    public static ReadDatasetBolt getTestInstance(String ecloudMcsAddress, OutputCollector outputCollector,
                                                  DataSetServiceClient datasetClient, FileServiceClient fileClient) {
        ReadDatasetBolt instance = new ReadDatasetBolt(ecloudMcsAddress);
        instance.outputCollector = outputCollector;
        instance.datasetClient = datasetClient;
        instance.fileClient = fileClient;
        return instance;

    }

    @Override
    public void execute(StormTaskTuple t) {
        //try data from DPS task
        String dpsTaskInputData = t.getParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA);

        if (dpsTaskInputData != null && !dpsTaskInputData.isEmpty()) {
            Type type = new TypeToken<Map<String, List<String>>>() {
            }.getType();
            Map<String, List<String>> fromJson = (Map<String, List<String>>) new Gson().fromJson(dpsTaskInputData, type);
            if (fromJson != null && fromJson.containsKey(DpsTask.DATASET_URLS)) {
                List<String> datasets = fromJson.get(DpsTask.DATASET_URLS);
                if (!datasets.isEmpty()) {
                    emitFilesFromDataSets(t, datasets);
                    return;
                }
            }
        } else {
            String message = "No dataset were provided";
            LOGGER.warn(message);
            emitDropNotification(t.getTaskId(), t.getFileUrl(), message, t.getParameters().toString());
            endTask(t.getTaskId(), message, TaskState.DROPPED, new Date());
            return;
        }

    }

    @Override
    public void prepare() {
        datasetClient = new DataSetServiceClient(ecloudMcsAddress);
        fileClient = new FileServiceClient(ecloudMcsAddress);
    }

    private void emitFilesFromDataSets(StormTaskTuple t, List<String> dataSets) {
        String representationName = t.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
        String authorizationHeader = t.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER);
        fileClient.useAuthorizationHeader(authorizationHeader);
        datasetClient.useAuthorizationHeader(authorizationHeader);
        String fileUrl = null;
        for (String dataSet : dataSets) {
            try {
                StormTaskTuple stormTaskTuple = new Cloner().deepClone(t);  //without cloning every emitted tuple will have the same object!!!
                UrlParser urlParser = new UrlParser(dataSet);
                if (urlParser.isUrlToDataset()) {
                    List<Representation> representations = datasetClient.getDataSetRepresentations(urlParser.getPart(UrlPart.DATA_PROVIDERS),
                            urlParser.getPart(UrlPart.DATA_SETS));
                    for (Representation representation : representations) {
                        if (representationName == null || representation.getRepresentationName().equals(representationName)) {
                            for (File file : representation.getFiles()) {
                                try {
                                    fileUrl = fileClient.getFileUri(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), file.getFileName()).toString();
                                    LOGGER.info("HERE THE LINK: " + fileUrl);
                                    InputStream is = fileClient.getFile(fileUrl);
                                    stormTaskTuple.setFileData(is);
                                    stormTaskTuple.setFileUrl(fileUrl);
                                    outputCollector.emit(inputTuple, stormTaskTuple.toStormTuple());
                                } catch (RepresentationNotExistsException | FileNotExistsException |
                                        WrongContentRangeException ex) {
                                    LOGGER.warn("Can not retrieve file at {}", fileUrl);
                                    emitDropNotification(t.getTaskId(), fileUrl, "Can not retrieve file", "");
                                } catch (DriverException | MCSException | IOException ex) {
                                    LOGGER.error("ReadFileBolt error:" + ex.getMessage());
                                    emitErrorNotification(t.getTaskId(), fileUrl, ex.getMessage(), t.getParameters().toString());
                                }
                            }
                        }
                    }
                } else {
                    LOGGER.warn("dataset url is not formulated correctly {}", dataSet);
                    emitDropNotification(t.getTaskId(), dataSet, "dataset url is not formulated correctly", "");
                }
            } catch (DataSetNotExistsException ex) {
                LOGGER.warn("Provided dataset is not existed {}", dataSet);
                emitDropNotification(t.getTaskId(), dataSet, "Can not retrieve a dataset", "");
            } catch (MalformedURLException | MCSException ex) {
                LOGGER.error("ReadFileBolt error:" + ex.getMessage());
                emitErrorNotification(t.getTaskId(), dataSet, ex.getMessage(), t.getParameters().toString());


            }
        }
    }
}

