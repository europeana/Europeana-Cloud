package eu.europeana.cloud.service.dps.services.task.postprocessors;

import com.google.common.collect.Iterators;
import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.commons.urls.DataSetUrlParser;
import eu.europeana.cloud.service.commons.urls.RepresentationParser;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.jvnet.hk2.annotations.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static org.springframework.http.HttpHeaders.AUTHORIZATION;

@Service
public class HarvestingPostProcessor implements TaskPostProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(HarvestingPostProcessor.class);

    private final HarvestedRecordsDAO harvestedRecordsDAO;

    private final ProcessedRecordsDAO processedRecordsDAO;

    private final RecordServiceClient recordServiceClient;

    private final RevisionServiceClient revisionServiceClient;

    private final DataSetServiceClient dataSetServiceClient;

    private final UISClient uisClient;

    private final TaskStatusUpdater taskStatusUpdater;

    public HarvestingPostProcessor(HarvestedRecordsDAO harvestedRecordsDAO,
                                   ProcessedRecordsDAO processedRecordsDAO,
                                   RecordServiceClient recordServiceClient,
                                   RevisionServiceClient revisionServiceClient,
                                   UISClient uisClient,
                                   DataSetServiceClient dataSetServiceClient,
                                   TaskStatusUpdater taskStatusUpdater) {
        this.harvestedRecordsDAO = harvestedRecordsDAO;
        this.processedRecordsDAO = processedRecordsDAO;
        this.recordServiceClient = recordServiceClient;
        this.revisionServiceClient = revisionServiceClient;
        this.uisClient = uisClient;
        this.dataSetServiceClient = dataSetServiceClient;
        this.taskStatusUpdater = taskStatusUpdater;
    }

    @Override
    public void execute(DpsTask dpsTask) {
        /* Plan:
        1. Iterate over oll records from table harvested_records where latest_harvest_date<task_execution_date
        2. For each europeana_id:
            - find cloud_id
            - create representation version
            - add revision (taken from task definition (output_revision))
            - add created representation version to dataset (dataset taken from task definition (output dataset))
        3. Change task status to PROCESSED;
         */
        Iterator<HarvestedRecord> it = fetchDeletedRecords(dpsTask);
        while (it.hasNext()) {
            HarvestedRecord harvestedRecord = it.next();
            if (!isRecordProcessed(dpsTask, harvestedRecord)) {
                addRecordToTaskOutputRevision(dpsTask, harvestedRecord);
                setRecordProcessed(dpsTask, harvestedRecord);
                LOGGER.info("Added deleted record {} to revision, taskId={}", harvestedRecord, dpsTask.getTaskId());
            } else {
                LOGGER.info("Omitted record {} cause it was already added to revision, taskId={}", harvestedRecord, dpsTask.getTaskId());
            }

        }
        taskStatusUpdater.setTaskCompletelyProcessed(dpsTask.getTaskId(), "PROCESSED");
    }

    private void addRecordToTaskOutputRevision(DpsTask dpsTask, HarvestedRecord harvestedRecord) {
        try {
            String cloudId = findCloudId(dpsTask, harvestedRecord);

            Representation representation = createRepresentationVersion(dpsTask, cloudId);

            addRevisionToRepresentation(dpsTask, representation);

            assignRepresentationToDatasets(dpsTask, representation);

        } catch (CloudException | MCSException | MalformedURLException e) {
            throw new PostProcessingException("Could not add deleted record id=" + harvestedRecord.getRecordLocalId()
                    + " to task result revision! taskId=" + dpsTask.getTaskId(), e);
        }

    }

    private Iterator<HarvestedRecord> fetchDeletedRecords(DpsTask task) {
        Date harvestDate = DateHelper.parseISODate(task.getParameter(PluginParameterKeys.HARVEST_DATE));
        Iterator<HarvestedRecord> allRecords =
                harvestedRecordsDAO.findDatasetRecords(task.getParameter(PluginParameterKeys.METIS_DATASET_ID));
        return Iterators.filter(allRecords, theRecord -> theRecord.getLatestHarvestDate().before(harvestDate));
    }


    private String findCloudId(DpsTask dpsTask, HarvestedRecord harvestedRecord) throws CloudException {
        String providerId = dpsTask.getParameter(PluginParameterKeys.PROVIDER_ID);
        return uisClient.getCloudId(providerId, harvestedRecord.getRecordLocalId(),
                AUTHORIZATION, dpsTask.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER)).getId();
    }

    private Representation createRepresentationVersion(DpsTask dpsTask, String cloudId) throws MCSException, MalformedURLException {
        String providerId = dpsTask.getParameter(PluginParameterKeys.PROVIDER_ID);
        String representationName = dpsTask.getParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME);
        URI representationUri = recordServiceClient.createRepresentation(cloudId, representationName, providerId,
                AUTHORIZATION, dpsTask.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER));
        return RepresentationParser.parseResultUrl(representationUri);
    }

    private void addRevisionToRepresentation(DpsTask dpsTask, Representation representation) throws MCSException {
        Revision revision = new Revision(dpsTask.getOutputRevision());
        revision.setDeleted(true);
        revisionServiceClient.addRevision(representation.getCloudId(), representation.getRepresentationName(),
                representation.getVersion(), revision, AUTHORIZATION, dpsTask.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER));
    }

    private void assignRepresentationToDatasets(DpsTask dpsTask, Representation representation) throws MalformedURLException, MCSException {
        List<DataSet> datasets = DataSetUrlParser.parseList(dpsTask.getParameter(PluginParameterKeys.OUTPUT_DATA_SETS));
        for (DataSet dataset : datasets) {
            dataSetServiceClient.assignRepresentationToDataSet(dataset.getProviderId(), dataset.getId(),
                    representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(),
                    AUTHORIZATION, dpsTask.getParameter(PluginParameterKeys.AUTHORIZATION_HEADER));
        }
    }


    private void setRecordProcessed(DpsTask dpsTask, HarvestedRecord harvestedRecord) {
        processedRecordsDAO.insert(ProcessedRecord.builder().taskId(dpsTask.getTaskId())
                .recordId(harvestedRecord.getRecordLocalId()).state(RecordState.SUCCESS).starTime(new Date()).build());
    }

    private boolean isRecordProcessed(DpsTask dpsTask, HarvestedRecord harvestedRecord) {
        return processedRecordsDAO.selectByPrimaryKey(dpsTask.getTaskId(), harvestedRecord.getRecordLocalId())
                .map(theRecord -> theRecord.getState() == RecordState.SUCCESS)
                .orElse(false);
    }
}
