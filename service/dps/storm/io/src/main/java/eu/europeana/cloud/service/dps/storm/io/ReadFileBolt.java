package eu.europeana.cloud.service.dps.storm.io;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rits.cloning.Cloner;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.Map;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.mcs.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.dps.DpsTask;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

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
    private final String username;
    private final String password;

    private FileServiceClient fileClient;
    private DataSetServiceClient dataSetClient;

    public ReadFileBolt(String ecloudMcsAddress, String username, String password) {
        this.ecloudMcsAddress = ecloudMcsAddress;
        this.username = username;
        this.password = password;
    }

    @Override
    public void prepare() {
        fileClient = new FileServiceClient(ecloudMcsAddress, username, password);
        dataSetClient = new DataSetServiceClient(ecloudMcsAddress, username, password);
    }

    @Override
    public void execute(StormTaskTuple t) {
        String fileUrl = t.getFileUrl();
        if (fileUrl == null || fileUrl.isEmpty()) {
            //try data from DPS task
            String dpsTaskInputData = t.getParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA);
            if (dpsTaskInputData != null && !dpsTaskInputData.isEmpty()) {
                Type type = new TypeToken<Map<String, List<String>>>() {
                }.getType();
                Map<String, List<String>> fromJson = (Map<String, List<String>>) new Gson().fromJson(dpsTaskInputData, type);
                if (fromJson != null && fromJson.containsKey(DpsTask.FILE_URLS)) {
                    List<String> files = fromJson.get(DpsTask.FILE_URLS);
                    if (!files.isEmpty()) {
                        emitFiles(t, files);
                        outputCollector.ack(inputTuple);
                        return;
                    }
                } else {
                    if (fromJson != null && fromJson.containsKey(DpsTask.DATASET_URLS)) {
                        List<String> dataSets = fromJson.get(DpsTask.DATASET_URLS);
                        if (!dataSets.isEmpty()) {
                            emitFilesFromDataSets(t, dataSets);
                            outputCollector.ack(inputTuple);
                            return;
                        }
                    }

                }
            }

            String message = "No URL for retrieve file.";
            LOGGER.warn(message);
            emitDropNotification(t.getTaskId(), "", message, t.getParameters().toString());
            emitBasicInfo(t.getTaskId(), 1);
            outputCollector.ack(inputTuple);
            return;
        }

        List<String> files = new ArrayList<>();
        files.add(fileUrl);
        emitFiles(t, files);

        outputCollector.ack(inputTuple);
    }

    private void emitFiles(StormTaskTuple t, List<String> files) {
        emitBasicInfo(t.getTaskId(), files.size());
        StormTaskTuple tt;

        for (String file : files) {
            tt = new Cloner().deepClone(t);  //without cloning every emitted tuple will have the same object!!!

            try {

                LOGGER.info("HERE THE LINK: " + file);

                InputStream is = fileClient.getFile(file);

                tt.setFileData(is);

                Map<String, String> parsedUri = FileServiceClient.parseFileUri(file);
                tt.addParameter(PluginParameterKeys.CLOUD_ID, parsedUri.get(ParamConstants.P_CLOUDID));
                tt.addParameter(PluginParameterKeys.REPRESENTATION_NAME, parsedUri.get(ParamConstants.P_REPRESENTATIONNAME));
                tt.addParameter(PluginParameterKeys.REPRESENTATION_VERSION, parsedUri.get(ParamConstants.P_VER));
                tt.addParameter(PluginParameterKeys.FILE_NAME, parsedUri.get(ParamConstants.P_FILENAME));

                tt.setFileUrl(file);

                outputCollector.emit(inputTuple, tt.toStormTuple());
            } catch (RepresentationNotExistsException | FileNotExistsException |
                    WrongContentRangeException ex) {
                LOGGER.warn("Can not retrieve file at {}", file);
                emitDropNotification(t.getTaskId(), file, "Can not retrieve file", "");
            } catch (DriverException | MCSException | IOException ex) {
                LOGGER.error("ReadFileBolt error:" + ex.getMessage());
                emitErrorNotification(t.getTaskId(), file, ex.getMessage(), t.getParameters().toString());
            }
        }
    }

    private void emitFilesFromDataSets(StormTaskTuple t, List<String> dataSets) {
        String representationName = t.getParameter(PluginParameterKeys.REPRESENTATION_NAME);
        int size = getFilesCountInsideDataSets(t, dataSets, representationName);
        emitBasicInfo(t.getTaskId(), size);
        String fileUrl = null;
        for (String dataSet : dataSets) {
            try {
                StormTaskTuple stormTaskTuple = new Cloner().deepClone(t);  //without cloning every emitted tuple will have the same object!!!
                UrlParser urlParser = new UrlParser(dataSet);
                if (urlParser.isUrlToDataset()) {
                    List<Representation> representations = dataSetClient.getDataSetRepresentations(urlParser.getPart(UrlPart.DATA_PROVIDERS),
                            urlParser.getPart(UrlPart.DATA_SETS));
                    for (Representation representation : representations) {
                        if (representationName == null || representation.getRepresentationName().equals(representationName)) {
                            List<File> files = representation.getFiles();
                            for (File file : files) {
                                try {
                                    fileUrl = fileClient.getFileUri(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), file.getFileName()).toString();
                                    LOGGER.info("HERE THE LINK: " + fileUrl);
                                    InputStream is = fileClient.getFile(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(), file.getFileName());
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
                    outputCollector.ack(inputTuple);
                }
            } catch (DataSetNotExistsException ex) {
                LOGGER.warn("Provided dataset is not existed {}", dataSet);
                emitDropNotification(t.getTaskId(), dataSet, "Can not retrieve a dataset", "");
                outputCollector.ack(inputTuple);
            } catch (MalformedURLException | MCSException ex) {
                LOGGER.error("ReadFileBolt error:" + ex.getMessage());
                emitErrorNotification(t.getTaskId(), dataSet, ex.getMessage(), t.getParameters().toString());
                outputCollector.ack(inputTuple);
            }
        }
    }

    private int getFilesCountInsideDataSets(StormTaskTuple t, List<String> dataSets, String representationName) {
        int size = 0;
        for (String dataSet : dataSets) {
            try {
                UrlParser urlParser = new UrlParser(dataSet);
                if (urlParser.isUrlToDataset()) {
                    List<Representation> representations = dataSetClient.getDataSetRepresentations(urlParser.getPart(UrlPart.DATA_PROVIDERS),
                            urlParser.getPart(UrlPart.DATA_SETS));
                    for (Representation representation : representations) {
                        if (representationName == null || representation.getRepresentationName().equals(representationName))
                            size += representation.getFiles().size();
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("Error while counting the number of files inside datasets" + e.getMessage());
            }
        }
        return size;
    }
}