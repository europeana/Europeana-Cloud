package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DpsTaskValidatorForOaiTopologyTest {

  private DpsTask oaiTopologyTask;
  private DpsTask oaiTopologyTaskWithoutOutputDataset;
  private DpsTask oaiTopologyTaskWithoutHarvestDate;
  private DpsTask oaiTopologyIncrementalTaskWithSampleSize;
  private DpsTask oaiTopologyIncrementalTaskWithoutSampleSize;

  @BeforeEach
  void init() {
    //
    oaiTopologyTask = new DpsTask();
    oaiTopologyTask.setInput(OAIPMHHarvestingDetails.builder().repositoryUrl(
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"
    ).build());
    oaiTopologyTask.addParameter(PluginParameterKeys.HARVEST_DATE, "harvestDate");
    oaiTopologyTask.setOutput(prepareOutput().build());
    //
    oaiTopologyTaskWithoutOutputDataset = new DpsTask();
    oaiTopologyTaskWithoutOutputDataset.setInput(OAIPMHHarvestingDetails.builder().repositoryUrl(
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"
    ).build());
    oaiTopologyTaskWithoutOutputDataset.addParameter(PluginParameterKeys.HARVEST_DATE, "harvestDate");
    oaiTopologyTaskWithoutOutputDataset.setOutput(prepareOutput().batchId(null).build());
    //
    oaiTopologyTaskWithoutHarvestDate = new DpsTask();
    oaiTopologyTaskWithoutHarvestDate.setInput(OAIPMHHarvestingDetails.builder().repositoryUrl(
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"
    ).build());
    oaiTopologyTaskWithoutHarvestDate.setOutput(prepareOutput().build());
    //
    oaiTopologyIncrementalTaskWithSampleSize = new DpsTask();
    oaiTopologyIncrementalTaskWithSampleSize.setInput(OAIPMHHarvestingDetails.builder().repositoryUrl(
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"
    ).build());
    oaiTopologyIncrementalTaskWithSampleSize.addParameter(PluginParameterKeys.HARVEST_DATE, "harvestDate");
    oaiTopologyIncrementalTaskWithSampleSize.addParameter(PluginParameterKeys.INCREMENTAL_HARVEST, "true");
    oaiTopologyIncrementalTaskWithSampleSize.addParameter(PluginParameterKeys.SAMPLE_SIZE, "10");
    oaiTopologyIncrementalTaskWithSampleSize.setOutput(prepareOutput().build());

    oaiTopologyIncrementalTaskWithoutSampleSize = new DpsTask();
    oaiTopologyIncrementalTaskWithoutSampleSize.setInput(OAIPMHHarvestingDetails.builder().repositoryUrl(
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"
    ).build());
    oaiTopologyIncrementalTaskWithoutSampleSize.addParameter(PluginParameterKeys.HARVEST_DATE, "harvestDate");
    oaiTopologyIncrementalTaskWithoutSampleSize.addParameter(PluginParameterKeys.INCREMENTAL_HARVEST, "true");
    oaiTopologyIncrementalTaskWithoutSampleSize.setOutput(prepareOutput().build());
  }

  @Test
  void shouldValidateTaskForOAITopology() throws DpsTaskValidationException {
    DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType("oai_topology_repository_urls");
    validator.validate(oaiTopologyTask);
  }

  @Test
  void shouldValidateTaskForOAITopologyWithZeroOutputDatasets() {
    try {
      DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType("oai_topology_repository_urls");
      validator.validate(oaiTopologyTaskWithoutOutputDataset);

    } catch (Exception e) {
      assertTrue(e.getMessage().contains("datasetId"));

    }
  }

  @Test
  void shouldValidateTasksWithoutHarvestDate() {
    DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType("oai_topology_repository_urls");
    assertThrows(DpsTaskValidationException.class, () -> validator.validate(oaiTopologyTaskWithoutHarvestDate));
  }


  @Test
  void shouldValidateIncrementalTaskForOAITopologyWithSampleSize() {
    try {
      DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType("oai_topology_repository_urls");
      validator.validate(oaiTopologyIncrementalTaskWithSampleSize);

    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Incremental harvesting could not set SAMPLE_SIZE"));

    }
  }

  @Test
  void shouldValidateIncrementalTaskForOAITopologyWithoutSampleSize() throws DpsTaskValidationException {
    DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType("oai_topology_repository_urls");
    validator.validate(oaiTopologyIncrementalTaskWithoutSampleSize);
  }

  private BatchInfo.BatchInfoBuilder prepareOutput() {
    return BatchInfo.builder()
                              .providerId("providerID")
                              .batchId("datasetId")
                              .representationName("representationName");

  }
}
