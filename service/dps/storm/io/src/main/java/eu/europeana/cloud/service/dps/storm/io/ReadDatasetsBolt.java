package eu.europeana.cloud.service.dps.storm.io;

import backtype.storm.task.OutputCollector;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.*;

/**
 * Read datasets and emit every dataset as a separate {@link StormTaskTuple}.
 */
public class ReadDatasetsBolt extends AbstractDpsBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadDatasetsBolt.class);

    /**
     * Constructor of ReadDatasetBolt.
     */
    public ReadDatasetsBolt() {

    }

    /**
     * Should be used only on tests.
     */
    public static ReadDatasetsBolt getTestInstance(OutputCollector outputCollector) {
        ReadDatasetsBolt instance = new ReadDatasetsBolt();
        instance.outputCollector = outputCollector;
        return instance;
    }

    @Override
    public void prepare() {

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
                    emitSingleDataSetFromDataSets(t, datasets);
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

    private void emitSingleDataSetFromDataSets(StormTaskTuple t, List<String> dataSets) {
        for (String dataSet : dataSets) {
            StormTaskTuple next = buildStormTaskTuple(t, dataSet);
            outputCollector.emit(inputTuple, next.toStormTuple());
        }
    }

    private StormTaskTuple buildStormTaskTuple(StormTaskTuple t, String dataSet) {
        StormTaskTuple stormTaskTuple = new Cloner().deepClone(t);
        Map<String, List<String>> dpsTaskInputData = new HashMap<>();
        dpsTaskInputData.put(DpsTask.DATASET_URLS, Arrays.asList(dataSet));
        stormTaskTuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, new Gson().toJson(dpsTaskInputData));
        return stormTaskTuple;
    }
}

