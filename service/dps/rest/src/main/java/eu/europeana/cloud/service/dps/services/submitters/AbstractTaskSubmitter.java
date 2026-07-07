package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.mcs.exception.MCSException;

import java.time.Instant;

public abstract class AbstractTaskSubmitter implements TaskSubmitter {
    private final DataSetServiceClient dataSetServiceClient;

    public AbstractTaskSubmitter(DataSetServiceClient dataSetServiceClient) {
        this.dataSetServiceClient = dataSetServiceClient;
    }


    void createDateSetIfNeeded(DpsTask dpsTask) throws TaskSubmissionException {
        try {
            if(dpsTask.getResultsBatch() instanceof BatchInfo output){
                if (!dataSetServiceClient.datasetExists(output.getProviderId(), output.getBatchId())) {
                    dataSetServiceClient.createDataSet(output.getProviderId(), output.getBatchId(), createDescription(dpsTask));
                }
            }
        } catch (MCSException e) {
            throw new TaskSubmissionException("Couldn't connect to mcs to verify dataSet exists for task with id %s!".formatted(dpsTask.getTaskId()), e);
        }
    }

    private String createDescription(DpsTask dpsTask) {
        return "Output batch of the task named: " + dpsTask.getTaskName()
            + ", id: " + dpsTask.getTaskId()
            + ", run for Metis dataset: " + dpsTask.getParameter(PluginParameterKeys.METIS_DATASET_ID)
            + ", at: "+ Instant.now();
    }
}
