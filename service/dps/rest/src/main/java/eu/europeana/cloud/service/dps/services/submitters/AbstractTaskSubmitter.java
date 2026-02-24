package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.mcs.exception.MCSException;

import java.net.MalformedURLException;
import java.util.List;

import static eu.europeana.cloud.service.dps.utils.DpsTaskDataSetUtil.getDatasetOutOfDpsTask;

public abstract class AbstractTaskSubmitter implements TaskSubmitter {
    private final DataSetServiceClient dataSetServiceClient;

    public AbstractTaskSubmitter(DataSetServiceClient dataSetServiceClient) {
        this.dataSetServiceClient = dataSetServiceClient;
    }


    void createDateSetIfNeeded(DpsTask dpsTask) throws TaskSubmissionException {
        try {
            List<DataSet> dataSets = getDatasetOutOfDpsTask(dpsTask);
            for (DataSet dataSet :
                    dataSets) {
                if (!dataSetServiceClient.datasetExists(dataSet.getProviderId(), dataSet.getId())) {
                    dataSetServiceClient.createDataSet(dataSet.getProviderId(), dataSet.getId(), dataSet.getDescription());
                }
            }
        } catch (MCSException e) {
            throw new TaskSubmissionException("Couldn't connect to mcs to verify dataSet exists for task with id %s!".formatted(dpsTask.getTaskId()), e);
        } catch (MalformedURLException e) {
            throw new TaskSubmissionException("Couldn't parse dataset Uri for given task with id %s!".formatted(dpsTask.getTaskId()), e);
        }
    }
}
