package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.job;

import com.google.common.base.Throwables;
import com.rits.cloning.Cloner;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.oaipmh.Harvester;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.oaipmh.Harvester.CancelTrigger;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterException;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.schema.SchemaFactory;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.schema.SchemaHandler;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
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
    public Void call() {
        try {
            execute(stormTaskTuple);
        } catch (Exception e) {
            cassandraTaskInfoDAO.dropTask(stormTaskTuple.getTaskId(), "The task was dropped because of " + e.getMessage() + ". The full exception is" + Throwables.getStackTraceAsString(e), TaskState.DROPPED.toString());
        }
        return null;
    }


    private void execute(StormTaskTuple stormTaskTuple) throws InterruptedException, HarvesterException {
        startProgress(stormTaskTuple.getTaskId());
        SchemaHandler schemaHandler = SchemaFactory.getSchemaHandler(stormTaskTuple, DEFAULT_RETRIES, SLEEP_TIME);
        Set<String> schemas = schemaHandler.getSchemas(stormTaskTuple);
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = stormTaskTuple.getSourceDetails();
        int expectedSize = 0;

        Date fromDate = oaipmhHarvestingDetails.getDateFrom();
        Date untilDate = oaipmhHarvestingDetails.getDateUntil();
        Set<String> sets = oaipmhHarvestingDetails.getSets();

        for (String schema : schemas) {
            if (sets == null || sets.isEmpty()) {
                expectedSize += harvestIdentifiers(schema, null, fromDate, untilDate, stormTaskTuple);
            } else {
                for (String set : sets) {
                    expectedSize += harvestIdentifiers(schema, set, fromDate, untilDate, stormTaskTuple);
                }
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

    private int harvestIdentifiers(String schema, String dataset, Date fromDate, Date untilDate,
            final StormTaskTuple stormTaskTuple) throws InterruptedException, HarvesterException {
        final CancelTrigger cancelTrigger = new CancelTrigger() {
            @Override
            public boolean shouldCancel() {
                return taskStatusChecker.hasKillFlag(stormTaskTuple.getTaskId());
            }
        };
        final String fileUrl = stormTaskTuple.getFileUrl();
        final Set<String> excludedSets = stormTaskTuple.getSourceDetails().getExcludedSets();
        final List<String> identifiers = new Harvester(DEFAULT_RETRIES, SLEEP_TIME)
                .harvestIdentifiers(schema, dataset, fromDate, untilDate, fileUrl, excludedSets,
                        cancelTrigger);
        for (String identifier:identifiers){
            fillIdentifiersQueue(stormTaskTuple, identifier, schema);
        }
        return identifiers.size();
    }

    private void fillIdentifiersQueue(StormTaskTuple stormTaskTuple, String identifier, String schema) throws InterruptedException {
        StormTaskTuple tuple = new Cloner().deepClone(stormTaskTuple);
        tuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, identifier);
        tuple.addParameter(PluginParameterKeys.SCHEMA_NAME, schema);
        tuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, stormTaskTuple.getFileUrl());
        tuple.setFileUrl(identifier);
        oaiIdentifiers.put(tuple);
    }
}

