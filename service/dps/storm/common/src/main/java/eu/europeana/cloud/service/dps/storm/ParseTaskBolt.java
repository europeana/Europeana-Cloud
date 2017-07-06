package eu.europeana.cloud.service.dps.storm;


import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static eu.europeana.cloud.service.dps.InputDataType.DATASET_URLS;
import static eu.europeana.cloud.service.dps.InputDataType.FILE_URLS;

/**
 * This bolt is responsible for convert {@link DpsTask} to {@link StormTaskTuple} and it emits result to specific storm stream.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class ParseTaskBolt extends BaseRichBolt {
    protected Map stormConfig;
    protected TopologyContext topologyContext;
    protected OutputCollector outputCollector;
    private static final Logger LOGGER = LoggerFactory.getLogger(ParseTaskBolt.class);

    public static final String NOTIFICATION_STREAM_NAME = AbstractDpsBolt.NOTIFICATION_STREAM_NAME;

    public final Map<String, String> routingRules;

    /**
     * Constructor for ParseTaskBolt with routing.
     * Task is dropped if TaskName is not in routingRules.
     *
     * @param routingRules routing table in the form ("TaskName": "StreamName")
     */
    public ParseTaskBolt(Map<String, String> routingRules) {
        this.routingRules = routingRules;
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
        Date startTime = new Date();

        StormTaskTuple stormTaskTuple = new StormTaskTuple(
                task.getTaskId(),
                task.getTaskName(),
                null, null, taskParameters, task.getOutputRevision(), task.getHarvestingDetails());
        String stream = getStream(task);
        if (stream != null) {
            String dataEntry = convertListToString(task.getDataEntry(InputDataType.valueOf(stream)));
            stormTaskTuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, dataEntry);
            updateTask(task.getTaskId(), "", TaskState.CURRENTLY_PROCESSING, startTime);
            outputCollector.emit(stream, tuple, stormTaskTuple.toStormTuple());
        } else {
            String message = "The taskType is not recognised!";
            LOGGER.warn(message);
            emitDropNotification(task.getTaskId(), "", message,
                    taskParameters != null ? taskParameters.toString() : "");
            endTask(task.getTaskId(), message, TaskState.DROPPED, new Date());
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
        if (task.getInputData().get(FILE_URLS) != null)
            return FILE_URLS.name();
        else if (task.getInputData().get(DATASET_URLS) != null)
            return DATASET_URLS.name();
        return null;

    }

    private String convertListToString(List<String> list) {
        String listString = list.toString();
        return listString.substring(1, listString.length() - 1);

    }

    @Override
    public void prepare(Map stormConfig, TopologyContext tc, OutputCollector oc) {
        this.stormConfig = stormConfig;
        this.topologyContext = tc;
        this.outputCollector = oc;
    }
}
