package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.services.DatasetCleanerService;

public class IndexingPostProcessor implements TaskPostProcessor {

    private DatasetCleanerService datasetCleanerService;

    public IndexingPostProcessor(DatasetCleanerService datasetCleanerService) {
        this.datasetCleanerService = datasetCleanerService;
    }

    @Override
    public void execute(DpsTask dpsTask) {
        datasetCleanerService.clean(String.valueOf(dpsTask.getTaskId()), new DataSetCleanerParameters());
    }
}
