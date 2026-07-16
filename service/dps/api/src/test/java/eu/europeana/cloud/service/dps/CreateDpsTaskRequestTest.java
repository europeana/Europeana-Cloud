package eu.europeana.cloud.service.dps;

import org.junit.jupiter.api.Test;

class CreateDpsTaskRequestTest {

  @Test
  void createHarvestingTaskWithOutputBatch() {
    CreateDpsTaskRequest task = new CreateDpsTaskRequest();
    OAIPMHHarvestingDetails input = new OAIPMHHarvestingDetails();
    input.setRepositoryUrl("https://oai.source.com");
    input.setSchema("edm");
    input.setSet("dataset");
    task.setSource(input);

    task.addParameter(PluginParameterKeys.METIS_DATASET_ID, "12212");
    task.addParameter(PluginParameterKeys.HARVEST_DATE, "2026-03-26T13:20:00.000Z");
    task.addParameter(PluginParameterKeys.INCREMENTAL_HARVEST, "true");

    task.setResultsProvider("metisProduction");

  }

  @Test
  void createNormalizationTaskWithInputBatch() {
    CreateDpsTaskRequest task = new CreateDpsTaskRequest();
    BatchInfo input = new BatchInfo();
    input.setProviderId("metisProduction");
    input.setBatchId("9cafe47c-1f42-4990-ba86-4f06a9ceb768");
    task.setSource(input);
    task.setResultsProvider("metisProduction");
  }
}