package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout;

import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
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

/**
 * Created by Tarek on 4/30/2018.
 */
public class OAISpout extends CustomKafkaSpout {

    private SpoutOutputCollector collector;
    private static final Logger LOGGER = LoggerFactory.getLogger(OAISpout.class);
    private SourceProvider sourceProvider;

    private DpsTask dpsTask;

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

        try {
            super.nextTuple();
            for (long taskId : cache.keySet()) {
                TaskSpoutInfo currentTask = cache.get(taskId);
                if (!currentTask.isStarted()) {
                    LOGGER.info("Start progressing for Task{}", currentTask);
                    startProgress(currentTask);
                    StormTaskTuple stormTaskTuple = new StormTaskTuple(
                            currentTask.getDpsTask().getTaskId(),
                            currentTask.getDpsTask().getTaskName(),
                            currentTask.getDpsTask().getDataEntry(InputDataType.REPOSITORY_URLS).get(0), null, currentTask.getDpsTask().getParameters(), currentTask.getDpsTask().getOutputRevision(), currentTask.getDpsTask().getHarvestingDetails());
                    execute(stormTaskTuple);
                    cache.remove(taskId);
                }
            }
        } catch (Exception e) {
            LOGGER.error("StaticDpsTaskSpout error: {}", e.getMessage());
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
        } catch (Exception e) {
            LOGGER.error("OAI Harvesting Spout error: {} ", e.getMessage());
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
        while (headerIterator.hasNext()) {
            Header header = headerIterator.next();
            if (filterHeader(header, excludedSets)) {
                emitIdentifier(stormTaskTuple, header.getIdentifier(), schema);
                count++;
            }
        }

        return count;
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

    private class CollectorWrapper extends SpoutOutputCollector {

        CollectorWrapper(ISpoutOutputCollector delegate) {
            super(delegate);
        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
            try {
                dpsTask = new ObjectMapper().readValue((String) tuple.get(0), DpsTask.class);
                if (dpsTask != null) {
                    long taskId = dpsTask.getTaskId();
                    if (cache.get(taskId) == null)
                        cache.put(taskId, new TaskSpoutInfo(dpsTask));
                }
            } catch (IOException e) {
                LOGGER.error(e.getMessage());
            }

            return Collections.emptyList();
        }
    }
}

