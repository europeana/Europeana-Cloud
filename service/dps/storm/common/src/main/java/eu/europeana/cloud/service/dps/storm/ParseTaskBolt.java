package eu.europeana.cloud.service.dps.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * This bolt is responsible for convert {@link DpsTask} to {@link StormTaskTuple} and it emits result to specific storm stream.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ParseTaskBolt extends BaseRichBolt {
    protected Map stormConfig;
    protected TopologyContext topologyContext;
    protected OutputCollector outputCollector;
    private final String datasetStream = "DATASET_STREAM";
    private final String fileStream = "FILE_STREAM";

    private static final Logger LOGGER = LoggerFactory.getLogger(ParseTaskBolt.class);

    public static final String NOTIFICATION_STREAM_NAME = AbstractDpsBolt.NOTIFICATION_STREAM_NAME;

    public final Map<String, String> routingRules;
    private final Map<String, String> prerequisites;

    /**
     * Constructor for ParseTaskBolt without routing and conditions.
     */
    public ParseTaskBolt() {
        this(null, null);
    }

    /**
     * Constructor for ParseTaskBolt with routing.
     * Task is dropped if TaskName is not in routingRules.
     *
     * @param routingRules  routing table in the form ("TaskName": "StreamName")
     * @param prerequisites Necessary parameters in DpsTask for continue. ("ParameterName": "CaseInsensitiveValue" or null if value is not important)
     *                      If parameter name is set in this structure and is not set in DpsTask or has a different value, than DpsTask will be dropped.
     */
    public ParseTaskBolt(Map<String, String> routingRules, Map<String, String> prerequisites) {
        this.routingRules = routingRules;
        this.prerequisites = prerequisites;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (routingRules != null) {
            for (Map.Entry<String, String> rule : routingRules.entrySet()) {
                declarer.declareStream(rule.getValue(), StormTaskTuple.getFields());
            }
        } else {
            declarer.declare(StormTaskTuple.getFields());
        }
        declarer.declareStream(NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
    }

    @Override
    public void execute(Tuple tuple) {
        ObjectMapper mapper = new ObjectMapper();
        DpsTask task;
        try {
            task = mapper.readValue(tuple.getString(0), DpsTask.class);
        } catch (IOException e) {
            LOGGER.error("Message '{}' rejected because: {}", tuple.getString(0), e.getMessage());
            outputCollector.ack(tuple);
            return;
        }
        Map<String, String> taskParameters = task.getParameters();
        String authenticationHeader = taskParameters.get(PluginParameterKeys.AUTHORIZATION_HEADER);
        if (authenticationHeader == null) {
            LOGGER.error("Message '{}' rejected because: {}", tuple.getString(0), "missing authentication Header");
            endTask(task.getTaskId(), "missing authentication Header", TaskState.DROPPED, new Date());
            outputCollector.ack(tuple);
            return;
        }

        StormTaskTuple stormTaskTuple = new StormTaskTuple(
                task.getTaskId(),
                task.getTaskName(),
                null, null, taskParameters);

        if (taskParameters != null) {
            String fileUrl = taskParameters.get(PluginParameterKeys.FILE_URL);
            if (fileUrl != null && !fileUrl.isEmpty()) {
                stormTaskTuple.setFileUrl(fileUrl);
            }

            String fileData = taskParameters.get(PluginParameterKeys.FILE_DATA);
            if (fileData != null && !fileData.isEmpty()) {
                stormTaskTuple.setFileData(fileData.getBytes(Charset.forName("UTF-8")));
            }
        }
        //add data from InputData as a parameter
        Map<String, List<String>> inputData = task.getInputData();
        if (inputData != null && !inputData.isEmpty()) {
            Type type = new TypeToken<Map<String, List<String>>>() {
            }.getType();
            stormTaskTuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, new Gson().toJson(inputData, type));
        }
        Date startTime = new Date();
        //use specific streams or default strem?
        if (routingRules != null) {
            String stream = getStream(task);
            if (stream != null) {
                updateTask(task.getTaskId(), "", TaskState.CURRENTLY_PROCESSING, startTime);
                outputCollector.emit(stream, tuple, stormTaskTuple.toStormTuple());
            } else {
                String message = "The taskType is not recognised!";
                LOGGER.warn(message);
                emitDropNotification(task.getTaskId(), "", message,
                        taskParameters != null ? taskParameters.toString() : "");
                endTask(task.getTaskId(), message, TaskState.DROPPED, new Date());
            }
        } else {
            updateTask(task.getTaskId(), "", TaskState.CURRENTLY_PROCESSING, startTime);
            outputCollector.emit(tuple, stormTaskTuple.toStormTuple());
        }
        outputCollector.ack(tuple);

    }

    private void emitDropNotification(long taskId, String resource,
                                      String message, String additionalInformations) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, States.DROPPED, message, additionalInformations);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }


    private void updateTask(long taskId, String info, TaskState state, Date startTime) {
        NotificationTuple nt = NotificationTuple.prepareUpdateTask(taskId, info, state, startTime);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }

    private void endTask(long taskId, String info, TaskState state, Date finishTime) {
        NotificationTuple nt = NotificationTuple.prepareEndTask(taskId, info, state, finishTime);
        outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }

    private String getStream(DpsTask task) {
        String stream = null;
        if (task.getInputData().get(DpsTask.FILE_URLS) != null)
            stream = fileStream;
        else if (task.getInputData().get(DpsTask.DATASET_URLS) != null)
            stream = datasetStream;
        return stream;

    }

    @Override
    public void prepare(Map stormConfig, TopologyContext tc, OutputCollector oc) {
        this.stormConfig = stormConfig;
        this.topologyContext = tc;
        this.outputCollector = oc;
    }
}
