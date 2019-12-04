package eu.europeana.cloud.service.dps.rest.oaiharvest;

import com.google.common.base.Throwables;
import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.rest.oaiharvest.schema.SchemaFactory;
import eu.europeana.cloud.service.dps.rest.oaiharvest.schema.SchemaHandler;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.dspace.xoai.model.oaipmh.Header;
import org.dspace.xoai.serviceprovider.ServiceProvider;
import org.dspace.xoai.serviceprovider.exceptions.BadArgumentException;
import org.dspace.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Created by Tarek on 10/25/2018.
 */
public class IdentifierHarvester implements Callable<Void> {
    private static final int DEFAULT_RETRIES = 3;
    private static final int SLEEP_TIME = 5000;
    private static final Logger LOGGER = LoggerFactory.getLogger(IdentifierHarvester.class);

    private String topologyName;
    private CassandraTaskInfoDAO cassandraTaskInfoDAO;
    private OAIItem oaiItem;
    private RecordExecutionSubmitService recordExecutionSubmitService;
    private TaskStatusChecker taskStatusChecker;

    public IdentifierHarvester(String topologyName, OAIItem oaiItem, CassandraTaskInfoDAO cassandraTaskInfoDAO, RecordExecutionSubmitService recordExecutionSubmitService, TaskStatusChecker taskStatusChecker) {
        this.topologyName = topologyName;
        this.oaiItem = oaiItem;
        this.cassandraTaskInfoDAO = cassandraTaskInfoDAO;
        this.recordExecutionSubmitService = recordExecutionSubmitService;
        this.taskStatusChecker = taskStatusChecker;
    }

    @Override
    public Void call() throws Exception {
        try {
            execute(oaiItem);
        } catch (Exception e) {
            cassandraTaskInfoDAO.dropTask(oaiItem.getTaskId(), "The task was dropped because of " + e.getMessage() + ". The full exception is" + Throwables.getStackTraceAsString(e), TaskState.DROPPED.toString());
        }
        return null;
    }

    public void harvest() {
        try {
            execute(oaiItem);
        } catch (Exception e) {
            cassandraTaskInfoDAO.dropTask(oaiItem.getTaskId(), "The task was dropped because of " + e.getMessage() + ". The full exception is" + Throwables.getStackTraceAsString(e), TaskState.DROPPED.toString());
        }
    }

    public void execute(OAIItem oaiItem) throws BadArgumentException, InterruptedException {
        startProgress(oaiItem.getTaskId());
        SchemaHandler schemaHandler = SchemaFactory.getSchemaHandler(oaiItem);
        Set<String> schemas = schemaHandler.getSchemas(oaiItem);
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = oaiItem.getSourceDetails();
        int expectedSize = 0;

        Date fromDate = oaipmhHarvestingDetails.getDateFrom();
        Date untilDate = oaipmhHarvestingDetails.getDateUntil();
        Set<String> sets = oaipmhHarvestingDetails.getSets();

        for (String schema : schemas) {
            if (sets == null || sets.isEmpty()) {
                expectedSize += harvestIdentifiers(schema, null, fromDate, untilDate, oaiItem);
            } else
                for (String set : sets) {
                    expectedSize += harvestIdentifiers(schema, set, fromDate, untilDate, oaiItem);
                }
        }

        updateTaskBasedOnExpectedSize(oaiItem, expectedSize);
    }

    private void updateTaskBasedOnExpectedSize(OAIItem oaiItem, int expectedSize) {
        if (expectedSize > 0) {
            cassandraTaskInfoDAO.setUpdateExpectedSize(oaiItem.getTaskId(), expectedSize);
        } else {
            cassandraTaskInfoDAO.dropTask(oaiItem.getTaskId(), "The task with the submitted parameters is empty", TaskState.DROPPED.toString());
        }
    }

    private void startProgress(long taskId) {
        LOGGER.info("Start progressing for Task with id {}", taskId);
        cassandraTaskInfoDAO.updateTask(taskId, "", String.valueOf(TaskState.CURRENTLY_PROCESSING), new Date());
    }


    private int harvestIdentifiers(String schema, String dataset, Date fromDate, Date untilDate, OAIItem oaiItem) throws InterruptedException
            , BadArgumentException {
        SourceProvider sourceProvider = new SourceProvider();
        OAIPMHHarvestingDetails sourceDetails = oaiItem.getSourceDetails();
        String url = oaiItem.getFileUrl();
        ListIdentifiersParameters parameters = configureParameters(schema, dataset, fromDate, untilDate);

        ServiceProvider sp = sourceProvider.provide(url);
        Iterator<Header> headerIterator = sp.listIdentifiers(parameters);
        Set<String> excludedSets = sourceDetails.getExcludedSets();

        return parseHeaders(headerIterator, excludedSets, oaiItem, schema);
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

    private void fillIdentifiersQueue(OAIItem oaiItem, String identifier, String schema) throws InterruptedException {
        OAIItem tuple = new Cloner().deepClone(oaiItem);
        tuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, identifier);
        tuple.addParameter(PluginParameterKeys.SCHEMA_NAME, schema);
        tuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, oaiItem.getFileUrl());
        tuple.setFileUrl(identifier);
        //oaiIdentifiers.put(tuple);
        recordExecutionSubmitService.submitRecord(new DpsRecord(oaiItem.getTaskId(), identifier), topologyName);
        LOGGER.info("************"+tuple.toString());
    }

    /**
     * Parse headers returned by the OAI-PMH source
     *
     * @param headerIterator iterator of headers returned by the source
     * @param excludedSets   sets to exclude
     * @param oaiItem tuple to be used for emitting identifier
     * @return number of harvested identifiers
     */
    private int parseHeaders(Iterator<Header> headerIterator, Set<String> excludedSets, OAIItem oaiItem, String schema) throws InterruptedException {
        int count = 0;
        if (headerIterator == null) {
            throw new IllegalArgumentException("Header iterator is null");
        }

        while (hasNext(headerIterator) && !taskStatusChecker.hasKillFlag(oaiItem.getTaskId())) {
            Header header = headerIterator.next();
            if (filterHeader(header, excludedSets)) {
                fillIdentifiersQueue(oaiItem, header.getIdentifier(), schema);
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

