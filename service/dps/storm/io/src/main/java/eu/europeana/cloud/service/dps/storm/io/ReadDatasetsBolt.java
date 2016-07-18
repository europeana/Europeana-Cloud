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
        Map<String, String> parameters = t.getParameters();
        List<String> datasets = Arrays.asList(parameters.get(PluginParameterKeys.DPS_TASK_INPUT_DATA).split("\\s*,\\s*"));
        if (datasets != null && !datasets.isEmpty()) {
            t.getParameters().remove(PluginParameterKeys.DPS_TASK_INPUT_DATA);
            emitSingleDataSetFromDataSets(t, datasets);
            return;
        } else {
            String message = "No URL to retrieve dataset.";
            LOGGER.warn(message);
            emitDropNotification(t.getTaskId(), "", message, t.getParameters().toString());
            endTask(t.getTaskId(), message, TaskState.DROPPED, new Date());
            return;
        }

    }

    private void emitSingleDataSetFromDataSets(StormTaskTuple t, List<String> dataSets) {
        for (String dataSet : dataSets) {
            StormTaskTuple stormTaskTuple = new Cloner().deepClone(t);
            stormTaskTuple.getParameters().put(PluginParameterKeys.DATASET_URL, dataSet);
            outputCollector.emit(inputTuple, stormTaskTuple.toStormTuple());
        }
    }
}

