package eu.europeana.cloud.service.dps;

import org.junit.jupiter.api.Test;

class DpsTaskTest {

  @Test
  void createHarvestingTaskWithOutputBatch() {
    DpsTask task = new DpsTask();
    OAIPMHHarvestingDetails input = new OAIPMHHarvestingDetails();
    input.setRepositoryUrl("https://oai.source.com");
    input.setSchema("edm");
    input.setSet("dataset");
    task.setInput(input);

    task.addParameter(PluginParameterKeys.METIS_DATASET_ID, "12212");
    task.addParameter(PluginParameterKeys.HARVEST_DATE, "2026-03-26T13:20:00.000Z");
    task.addParameter(PluginParameterKeys.INCREMENTAL_HARVEST, "true");

    task.setOutputProvider("metisProduction");

  }

  @Test
  void createNormalizationTaskWithInputBatch() {
    DpsTask task = new DpsTask();
    BatchInfo input = new BatchInfo();
    input.setProviderId("metisProduction");
    input.setBatchId("9cafe47c-1f42-4990-ba86-4f06a9ceb768");
    task.setInput(input);
    task.setOutputProvider("metisProduction");
  }
}