package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.CreateDpsTaskRequest;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DpsTaskValidatorForOaiTopologyTest {

  private static final String OUTPUT_PROVIDER = "providerID";
  private CreateDpsTaskRequest oaiTopologyTask;
  private CreateDpsTaskRequest oaiTopologyTaskWithoutOutputDataset;
  private CreateDpsTaskRequest oaiTopologyTaskWithoutHarvestDate;
  private CreateDpsTaskRequest oaiTopologyIncrementalTaskWithSampleSize;
  private CreateDpsTaskRequest oaiTopologyIncrementalTaskWithoutSampleSize;

  @BeforeEach
  void init() {
    //
    oaiTopologyTask = new CreateDpsTaskRequest();
    oaiTopologyTask.setInput(OAIPMHHarvestingDetails.builder().repositoryUrl(
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"
    ).build());
    oaiTopologyTask.addParameter(PluginParameterKeys.HARVEST_DATE, "harvestDate");
    oaiTopologyTask.setOutputProvider(OUTPUT_PROVIDER);
    //
    oaiTopologyTaskWithoutOutputDataset = new CreateDpsTaskRequest();
    oaiTopologyTaskWithoutOutputDataset.setInput(OAIPMHHarvestingDetails.builder().repositoryUrl(
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"
    ).build());
    oaiTopologyTaskWithoutOutputDataset.addParameter(PluginParameterKeys.HARVEST_DATE, "harvestDate");
    oaiTopologyTaskWithoutOutputDataset.setOutputProvider(OUTPUT_PROVIDER);
    //
    oaiTopologyTaskWithoutHarvestDate = new CreateDpsTaskRequest();
    oaiTopologyTaskWithoutHarvestDate.setInput(OAIPMHHarvestingDetails.builder().repositoryUrl(
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"
    ).build());
    oaiTopologyTaskWithoutHarvestDate.setOutputProvider(OUTPUT_PROVIDER);
    //
    oaiTopologyIncrementalTaskWithSampleSize = new CreateDpsTaskRequest();
    oaiTopologyIncrementalTaskWithSampleSize.setInput(OAIPMHHarvestingDetails.builder().repositoryUrl(
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"
    ).build());
    oaiTopologyIncrementalTaskWithSampleSize.addParameter(PluginParameterKeys.HARVEST_DATE, "harvestDate");
    oaiTopologyIncrementalTaskWithSampleSize.addParameter(PluginParameterKeys.INCREMENTAL_HARVEST, "true");
    oaiTopologyIncrementalTaskWithSampleSize.addParameter(PluginParameterKeys.SAMPLE_SIZE, "10");
    oaiTopologyIncrementalTaskWithSampleSize.setOutputProvider(OUTPUT_PROVIDER);

    oaiTopologyIncrementalTaskWithoutSampleSize = new CreateDpsTaskRequest();
    oaiTopologyIncrementalTaskWithoutSampleSize.setInput(OAIPMHHarvestingDetails.builder().repositoryUrl(
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"
    ).build());
    oaiTopologyIncrementalTaskWithoutSampleSize.addParameter(PluginParameterKeys.HARVEST_DATE, "harvestDate");
    oaiTopologyIncrementalTaskWithoutSampleSize.addParameter(PluginParameterKeys.INCREMENTAL_HARVEST, "true");
    oaiTopologyIncrementalTaskWithoutSampleSize.setOutputProvider(OUTPUT_PROVIDER);
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

}
