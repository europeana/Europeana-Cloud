package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout;

import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.CustomKafkaSpout;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.utils.TaskSpoutInfo;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.common.OAIHelper;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helpers.SourceProvider;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.schema.SchemaFactory;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.schema.SchemaHandler;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.codehaus.jackson.map.ObjectMapper;
import org.dspace.xoai.model.oaipmh.Header;
import org.dspace.xoai.serviceprovider.exceptions.BadArgumentException;
import org.dspace.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;

/**
 * Created by Tarek on 4/30/2018.
 */
public class OAISpout extends CustomKafkaSpout {

    private SpoutOutputCollector collector;
    private static final Logger LOGGER = LoggerFactory.getLogger(OAISpout.class);
    private SourceProvider sourceProvider;

    public static final int DEFAULT_RETRIES = 10;

    public static final int SLEEP_TIME = 5000;

    private transient ConcurrentHashMap<Long, TaskSpoutInfo> cache;

    public OAISpout(SpoutConfig spoutConf, String hosts, int port, String keyspaceName,
                    String userName, String password) {
        super(spoutConf, hosts, port, keyspaceName, userName, password);

    }


    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        cache = new ConcurrentHashMap<>(50);
        sourceProvider = new SourceProvider();
        super.open(conf, context, new CollectorWrapper(collector));
    }

    @Override
    public void nextTuple() {
        DpsTask dpsTask = null;

        try {
            super.nextTuple();
            for (long taskId : cache.keySet()) {
                TaskSpoutInfo currentTask = cache.get(taskId);
                if (!currentTask.isStarted()) {
                    LOGGER.info("Start progressing for Task with id {}", currentTask.getDpsTask().getTaskId());
                    startProgress(currentTask);
                    dpsTask = currentTask.getDpsTask();
                    OAIPMHHarvestingDetails oaipmhHarvestingDetails = dpsTask.getHarvestingDetails();
                    System.out.println("Th oai harvesting details " + oaipmhHarvestingDetails);
                    if (oaipmhHarvestingDetails == null)
                        oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
                    StormTaskTuple stormTaskTuple = new StormTaskTuple(
                            dpsTask.getTaskId(),
                            dpsTask.getTaskName(),
                            dpsTask.getDataEntry(InputDataType.REPOSITORY_URLS).get(0), null, dpsTask.getParameters(), dpsTask.getOutputRevision(), oaipmhHarvestingDetails);
                    execute(stormTaskTuple);
                    cache.remove(taskId);
                }
            }
        } catch (Exception e) {
            LOGGER.error("StaticDpsTaskSpout error: {}", e.getMessage());
            if (dpsTask != null)
                cassandraTaskInfoDAO.dropTask(dpsTask.getTaskId(), "The task was dropped because " + e.getMessage(), TaskState.DROPPED.toString());
        }
    }

    private void startProgress(TaskSpoutInfo taskInfo) {
        taskInfo.startTheTask();
        DpsTask task = taskInfo.getDpsTask();
        cassandraTaskInfoDAO.updateTask(task.getTaskId(), "", String.valueOf(TaskState.CURRENTLY_PROCESSING), new Date());

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(StormTaskTuple.getFields());
        declarer.declareStream(NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
    }


    public void execute(StormTaskTuple stormTaskTuple) {

        try {
            SchemaHandler schemaHandler = SchemaFactory.getSchemaHandler(stormTaskTuple);
            Set<String> schemas = schemaHandler.getSchemas(stormTaskTuple);
            OAIPMHHarvestingDetails oaipmhHarvestingDetails = stormTaskTuple.getSourceDetails();
            OAIHelper oaiHelper = new OAIHelper(stormTaskTuple.getFileUrl());
            int count = 0;
            Date fromDate = oaipmhHarvestingDetails.getDateFrom();

            if (fromDate == null) {
                fromDate = oaiHelper.getEarlierDate();
            }
            Date untilDate = oaipmhHarvestingDetails.getDateFrom();
            if (untilDate == null) {
                untilDate = new Date();
            }
            Set<String> sets = oaipmhHarvestingDetails.getSets();

            for (String schema : schemas) {
                if (sets == null || sets.isEmpty()) {
                    count += harvestIdentifiers(schema, null, fromDate, untilDate, oaiHelper.getGranularity().toString(), stormTaskTuple);
                } else
                    for (String set : sets) {
                        count += harvestIdentifiers(schema, set, fromDate, untilDate, oaiHelper.getGranularity().toString(), stormTaskTuple);
                    }
            }
            LOGGER.debug("Harvested " + count + " identifiers for source (" + stormTaskTuple.getSourceDetails() + ")");
            cache.get(stormTaskTuple.getTaskId()).setFileCount(count);
            cassandraTaskInfoDAO.setUpdateExpectedSize(stormTaskTuple.getTaskId(), count);
        } catch (BadArgumentException e) {
            LOGGER.error("OAI Harvesting Spout error: {} ", e.getMessage());
            emitErrorNotification(stormTaskTuple.getTaskId(), stormTaskTuple.getFileUrl(), "Error while Harvesting identifiers " + e.getMessage(), "");

        }
    }


    private int harvestIdentifiers(String schema, String dataset, Date fromDate, Date untilDate, String granularity, StormTaskTuple stormTaskTuple)
            throws BadArgumentException {
        OAIPMHHarvestingDetails sourceDetails = stormTaskTuple.getSourceDetails();
        String url = stormTaskTuple.getFileUrl();
        ListIdentifiersParameters parameters = configureParameters(schema, dataset, fromDate, untilDate, granularity);
        return parseHeaders(sourceProvider.provide(url).listIdentifiers(parameters), sourceDetails.getExcludedSets(), stormTaskTuple, schema);
    }

    /**
     * Configure request parameters
     *
     * @return object representing parameters for ListIdentifiers request
     */
    private ListIdentifiersParameters configureParameters(String schema, String dataset, Date fromDate, Date untilDate, String granularity) {
        ListIdentifiersParameters parameters = ListIdentifiersParameters.request()
                .withMetadataPrefix(schema);
        parameters.withGranularity(granularity);
        parameters.withFrom(fromDate);
        parameters.withUntil(untilDate);
        if (dataset != null)
            parameters.withSetSpec(dataset);

        return parameters;
    }

    private void emitIdentifier(StormTaskTuple stormTaskTuple, String identifier, String schema) {
        StormTaskTuple tuple = new Cloner().deepClone(stormTaskTuple);
        tuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, identifier);
        tuple.addParameter(PluginParameterKeys.SCHEMA_NAME, schema);
        tuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, stormTaskTuple.getFileUrl());
        tuple.setFileUrl(identifier);
        collector.emit(tuple.toStormTuple());
    }


    /**
     * Parse headers returned by the OAI-PMH source
     *
     * @param headerIterator iterator of headers returned by the source
     * @param excludedSets   sets to exclude
     * @param stormTaskTuple tuple to be used for emitting identifier
     * @return number of harvested identifiers
     */
    private int parseHeaders(Iterator<Header> headerIterator, Set<String> excludedSets, StormTaskTuple stormTaskTuple, String schema) {
        if (headerIterator == null) {
            throw new IllegalArgumentException("Header iterator is null");
        }

        int count = 0;
        while (hasNext(headerIterator) && !taskStatusChecker.hasKillFlag(stormTaskTuple.getTaskId())) {
            Header header = headerIterator.next();
            if (filterHeader(header, excludedSets)) {
                emitIdentifier(stormTaskTuple, header.getIdentifier(), schema);
                count++;
            }
        }

        return count;
    }

    private boolean hasNext(Iterator<Header> headerIterator) {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                return headerIterator.hasNext();
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while getting the next batch: {}", retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while getting the next batch");
                    throw e;
                }
            }
        }
    }

    protected void waitForSpecificTime() {
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            LOGGER.error(e1.getMessage());
        }
    }


    /**
     * Filter header by checking whether it belongs to any of excluded sets.
     *
     * @param header       header to filter
     * @param excludedSets sets to exclude
     */

    private boolean filterHeader(Header header, Set<String> excludedSets) {
        if (excludedSets != null && !excludedSets.isEmpty()) {
            for (String set : excludedSets) {
                if (header.getSetSpecs().contains(set)) {
                    return false;
                }
            }
        }
        return true;
    }

    private void emitErrorNotification(long taskId, String resource, String message, String additionalInformations) {
        NotificationTuple nt = NotificationTuple.prepareNotification(taskId,
                resource, States.ERROR, message, additionalInformations);
        collector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
    }

    private class CollectorWrapper extends SpoutOutputCollector {

        CollectorWrapper(ISpoutOutputCollector delegate) {
            super(delegate);
        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
            try {
                DpsTask dpsTask = new ObjectMapper().readValue((String) tuple.get(0), DpsTask.class);
                if (dpsTask != null) {
                    long taskId = dpsTask.getTaskId();
                    cache.putIfAbsent(taskId, new TaskSpoutInfo(dpsTask));
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }

            return Collections.emptyList();
        }
    }
}

