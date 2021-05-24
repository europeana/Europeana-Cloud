package eu.europeana.cloud.service.dps.services.task.postprocessors;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.mcs.exception.MCSException;

import java.util.Iterator;
import java.util.stream.StreamSupport;

public class DeletedRecordsForIncrementalHarvestingPostProcessor implements TaskPostprocessor {


    private final HarvestedRecordsDAO harvestedRecordsDAO;

    private final RecordServiceClient recordServiceClient;

    private final RevisionServiceClient revisionServiceClient;

    private final DataSetServiceClient dataSetServiceClient;

    private final UISClient uisClient;

    private final TaskStatusUpdater taskStatusUpdater;

    public DeletedRecordsForIncrementalHarvestingPostProcessor(HarvestedRecordsDAO harvestedRecordsDAO,RecordServiceClient recordServiceClient,
                                                               RevisionServiceClient revisionServiceClient,
                                                               UISClient uisClient,
                                                               DataSetServiceClient dataSetServiceClient,
                                                               TaskStatusUpdater taskStatusUpdater){
        this.harvestedRecordsDAO = harvestedRecordsDAO;
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
            - find clouud_id
            - create representation version
            - add revision (taken from task definition (output_revision))
            - add created representation version to dataset (dataset taken from task definition (output dataset))
        3. Change task status to PROCESSED;
        4. Update counter of deleted records;
         */
        Iterator<HarvestedRecord> datasetRecords = harvestedRecordsDAO.findDatasetRecords(dpsTask.getParameter(PluginParameterKeys.METIS_DATASET_ID));
        Iterable<HarvestedRecord> iterable = () -> datasetRecords;
        StreamSupport.stream(iterable.spliterator(), false)
                .filter(harvestedRecord -> harvestedRecord.getRecordLocalId().equals("sample"))
                .forEach(harvestedRecord -> {
                    try {
                        CloudId cloudId = uisClient.getCloudId("prov", "recordId");
                        recordServiceClient.createRepresentation(cloudId.getId(), "repName", "providerId", "key", "velue");
                        revisionServiceClient.addRevision(cloudId.getId(), "representationName", "representationName", new Revision(), "key", "velue");
                        dataSetServiceClient.assignRepresentationToDataSet("providerId","datasetId","cloudId","representationName","version","key","value");
                        taskStatusUpdater.setTaskCompletelyProcessed(1L,"PROCESSED");
                    } catch (CloudException | MCSException e) {
                        e.printStackTrace();
                    }
                });

        //


        //

    }
}
