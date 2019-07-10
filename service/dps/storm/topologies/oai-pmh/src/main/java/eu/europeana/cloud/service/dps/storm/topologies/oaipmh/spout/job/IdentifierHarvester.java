package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.job;

import com.google.common.base.Throwables;
import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helpers.SourceProvider;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.schema.SchemaFactory;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.schema.SchemaHandler;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.dspace.xoai.model.oaipmh.Header;
import org.dspace.xoai.serviceprovider.exceptions.BadArgumentException;
import org.dspace.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;

/**
 * Created by Tarek on 10/25/2018.
 */
public class IdentifierHarvester implements Callable<Void> {

    private CassandraTaskInfoDAO cassandraTaskInfoDAO;
    private ArrayBlockingQueue<StormTaskTuple> oaiIdentifiers;
    private StormTaskTuple stormTaskTuple;
    private static final int DEFAULT_RETRIES = 3;
    private static final int SLEEP_TIME = 5000;
    private static final Logger LOGGER = LoggerFactory.getLogger(IdentifierHarvester.class);
    private TaskStatusChecker taskStatusChecker;

    public IdentifierHarvester(StormTaskTuple stormTaskTuple, CassandraTaskInfoDAO cassandraTaskInfoDAO, ArrayBlockingQueue<StormTaskTuple> oaiIdentifiers, TaskStatusChecker taskStatusChecker) {
        this.stormTaskTuple = stormTaskTuple;
        this.cassandraTaskInfoDAO = cassandraTaskInfoDAO;
        this.oaiIdentifiers = oaiIdentifiers;
        this.taskStatusChecker = taskStatusChecker;
    }

    @Override
    public Void call() throws Exception {
        try {
            execute(stormTaskTuple);
        } catch (Exception e) {
            cassandraTaskInfoDAO.dropTask(stormTaskTuple.getTaskId(), "The task was dropped because of " + e.getMessage() + ". The full exception is" + Throwables.getStackTraceAsString(e), TaskState.DROPPED.toString());
        }
        return null;
    }


    public void execute(StormTaskTuple stormTaskTuple) throws BadArgumentException, InterruptedException {
        startProgress(stormTaskTuple.getTaskId());
        SchemaHandler schemaHandler = SchemaFactory.getSchemaHandler(stormTaskTuple);
        Set<String> schemas = schemaHandler.getSchemas(stormTaskTuple);
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = stormTaskTuple.getSourceDetails();
        int expectedSize = 0;

        Date fromDate = oaipmhHarvestingDetails.getDateFrom();
        Date untilDate = oaipmhHarvestingDetails.getDateUntil();
        Set<String> sets = oaipmhHarvestingDetails.getSets();

        for (String schema : schemas) {
            if (sets == null || sets.isEmpty()) {
                expectedSize += harvestIdentifiers(schema, null, fromDate, untilDate, stormTaskTuple);
            } else
                for (String set : sets) {
                    expectedSize += harvestIdentifiers(schema, set, fromDate, untilDate, stormTaskTuple);
                }
        }

        updateTaskBasedOnExpectedSize(stormTaskTuple, expectedSize);
    }

    private void updateTaskBasedOnExpectedSize(StormTaskTuple stormTaskTuple, int expectedSize) {
        if (expectedSize > 0)
            cassandraTaskInfoDAO.setUpdateExpectedSize(stormTaskTuple.getTaskId(), expectedSize);
        else
            cassandraTaskInfoDAO.dropTask(stormTaskTuple.getTaskId(), "The task with the submitted parameters is empty", TaskState.DROPPED.toString());
    }

    private void startProgress(long taskId) {
        LOGGER.info("Start progressing for Task with id {}", taskId);
        cassandraTaskInfoDAO.updateTask(taskId, "", String.valueOf(TaskState.CURRENTLY_PROCESSING), new Date());
    }


    private int harvestIdentifiers(String schema, String dataset, Date fromDate, Date untilDate, StormTaskTuple stormTaskTuple) throws InterruptedException
            , BadArgumentException {
        SourceProvider sourceProvider = new SourceProvider();
        OAIPMHHarvestingDetails sourceDetails = stormTaskTuple.getSourceDetails();
        String url = stormTaskTuple.getFileUrl();
        ListIdentifiersParameters parameters = configureParameters(schema, dataset, fromDate, untilDate);
        return parseHeaders(sourceProvider.provide(url).listIdentifiers(parameters), sourceDetails.getExcludedSets(), stormTaskTuple, schema);
    }

    /**
     * Configure request parameters
     *
     * @return object representing parameters for ListIdentifiers request
     */
    private ListIdentifiersParameters configureParameters(String schema, String dataset, Date fromDate, Date untilDate) {
        ListIdentifiersParameters parameters = ListIdentifiersParameters.request()
                .withMetadataPrefix(schema);

        if (fromDate != null)
            parameters.withFrom(fromDate);
        if (untilDate != null)
            parameters.withUntil(untilDate);
        if (dataset != null)
            parameters.withSetSpec(dataset);

        return parameters;
    }

    private void fillIdentifiersQueue(StormTaskTuple stormTaskTuple, String identifier, String schema) throws InterruptedException {
        StormTaskTuple tuple = new Cloner().deepClone(stormTaskTuple);
        tuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, identifier);
        tuple.addParameter(PluginParameterKeys.SCHEMA_NAME, schema);
        tuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, stormTaskTuple.getFileUrl());
        tuple.setFileUrl(identifier);
        oaiIdentifiers.put(tuple);

    }

    /**
     * Parse headers returned by the OAI-PMH source
     *
     * @param headerIterator iterator of headers returned by the source
     * @param excludedSets   sets to exclude
     * @param stormTaskTuple tuple to be used for emitting identifier
     * @return number of harvested identifiers
     */
    private int parseHeaders(Iterator<Header> headerIterator, Set<String> excludedSets, StormTaskTuple stormTaskTuple, String schema) throws InterruptedException {
        int count = 0;
        if (headerIterator == null) {
            throw new IllegalArgumentException("Header iterator is null");
        }

        while (hasNext(headerIterator) && !taskStatusChecker.hasKillFlag(stormTaskTuple.getTaskId())) {
            Header header = headerIterator.next();
            if (filterHeader(header, excludedSets)) {
                fillIdentifiersQueue(stormTaskTuple, header.getIdentifier(), schema);
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
                    LOGGER.warn("Error while getting the next batch: {}. Retries left {}. The cause of the error is {}", e.getMessage(), retries, e.getMessage() + " " + e.getCause());
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while getting the next batch {}", e.getMessage());
                    throw new IllegalStateException(" Error while getting the next batch of identifiers from the oai end-point.", e);
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
}

