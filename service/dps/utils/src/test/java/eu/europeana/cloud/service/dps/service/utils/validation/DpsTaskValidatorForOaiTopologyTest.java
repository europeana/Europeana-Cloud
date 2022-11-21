package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static eu.europeana.cloud.service.dps.InputDataType.REPOSITORY_URLS;

public class DpsTaskValidatorForOaiTopologyTest {

  private DpsTask oaiTopologyTask;
  private DpsTask oaiTopologyTaskWithoutOutputDataset;
  private DpsTask oaiTopologyTaskWithTwoRepositories;
  private DpsTask oaiTopologyTaskWithTwoOutputDatasets;
  private DpsTask oaiTopologyTaskWithoutHarvestDate;
  private DpsTask oaiTopologyIncrementalTaskWithSampleSize;
  private DpsTask oaiTopologyIncrementalTaskWithoutSampleSize;

  @Before
  public void init() {
    //
    oaiTopologyTask = new DpsTask();
    oaiTopologyTask.addDataEntry(REPOSITORY_URLS, Arrays.asList(
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
    oaiTopologyTask.addParameter(PluginParameterKeys.PROVIDER_ID, "providerID");
    oaiTopologyTask.addParameter(PluginParameterKeys.HARVEST_DATE, "harvestDate");
    oaiTopologyTask.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS,
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt");
    //
    oaiTopologyTaskWithoutOutputDataset = new DpsTask();
    oaiTopologyTaskWithoutOutputDataset.addDataEntry(REPOSITORY_URLS, Arrays.asList(
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
    oaiTopologyTaskWithoutOutputDataset.addParameter(PluginParameterKeys.HARVEST_DATE, "harvestDate");
    oaiTopologyTaskWithoutOutputDataset.addParameter(PluginParameterKeys.PROVIDER_ID, "providerID");
    //
    oaiTopologyTaskWithTwoRepositories = new DpsTask();
    oaiTopologyTaskWithTwoRepositories.addDataEntry(REPOSITORY_URLS, Arrays.asList(
        "http://iks-kbase.synat.pcss.pl:9090/mcs/records/JP46FLZLVI2UYV4JNHTPPAB4DGPESPY4SY4N5IUQK4SFWMQ3NUQQ/representations/tiff/versions/74c56880-7733-11e5-b38f-525400ea6731/files/f59753a5-6d75-4d48-9f4d-4690b671240c",
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
    oaiTopologyTaskWithTwoRepositories.addParameter(PluginParameterKeys.HARVEST_DATE, "harvestDate");
    oaiTopologyTaskWithTwoRepositories.addParameter(PluginParameterKeys.PROVIDER_ID, "providerID");
    //
    oaiTopologyTaskWithTwoOutputDatasets = new DpsTask();
    oaiTopologyTaskWithTwoOutputDatasets.addDataEntry(REPOSITORY_URLS, Arrays.asList(
        "http://iks-kbase.synat.pcss.pl:9090/mcs/records/JP46FLZLVI2UYV4JNHTPPAB4DGPESPY4SY4N5IUQK4SFWMQ3NUQQ/representations/tiff/versions/74c56880-7733-11e5-b38f-525400ea6731/files/f59753a5-6d75-4d48-9f4d-4690b671240c"));
    oaiTopologyTaskWithTwoOutputDatasets.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS,
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt,http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt");
    oaiTopologyTaskWithTwoOutputDatasets.addParameter(PluginParameterKeys.HARVEST_DATE, "harvestDate");
    oaiTopologyTaskWithTwoOutputDatasets.addParameter(PluginParameterKeys.PROVIDER_ID, "providerID");
    //
    oaiTopologyTaskWithoutHarvestDate = new DpsTask();
    oaiTopologyTaskWithoutHarvestDate.addDataEntry(REPOSITORY_URLS, Arrays.asList(
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
    oaiTopologyTaskWithoutHarvestDate.addParameter(PluginParameterKeys.PROVIDER_ID, "providerID");
    oaiTopologyTaskWithoutHarvestDate.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS,
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt");
    //
    oaiTopologyIncrementalTaskWithSampleSize = new DpsTask();
    oaiTopologyIncrementalTaskWithSampleSize.addDataEntry(REPOSITORY_URLS, Arrays.asList(
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
    oaiTopologyIncrementalTaskWithSampleSize.addParameter(PluginParameterKeys.HARVEST_DATE, "harvestDate");
    oaiTopologyIncrementalTaskWithSampleSize.addParameter(PluginParameterKeys.PROVIDER_ID, "providerID");
    oaiTopologyIncrementalTaskWithSampleSize.addParameter(PluginParameterKeys.INCREMENTAL_HARVEST, "true");
    oaiTopologyIncrementalTaskWithSampleSize.addParameter(PluginParameterKeys.SAMPLE_SIZE, "10");
    oaiTopologyIncrementalTaskWithSampleSize.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS,
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt");
    //
    oaiTopologyIncrementalTaskWithoutSampleSize = new DpsTask();
    oaiTopologyIncrementalTaskWithoutSampleSize.addDataEntry(REPOSITORY_URLS, Arrays.asList(
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
    oaiTopologyIncrementalTaskWithoutSampleSize.addParameter(PluginParameterKeys.HARVEST_DATE, "harvestDate");
    oaiTopologyIncrementalTaskWithoutSampleSize.addParameter(PluginParameterKeys.PROVIDER_ID, "providerID");
    oaiTopologyIncrementalTaskWithoutSampleSize.addParameter(PluginParameterKeys.INCREMENTAL_HARVEST, "true");
    oaiTopologyIncrementalTaskWithoutSampleSize.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS,
        "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt");
  }

  @Test
  public void shouldValidateTaskForOAITopology() throws DpsTaskValidationException {
    DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType("oai_topology_repository_urls");
    validator.validate(oaiTopologyTask);
  }

  @Test
  public void shouldValidateTaskForOAITopologyWithMoreThanOneRepositoryUrl() throws DpsTaskValidationException {
    try {
      DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType("oai_topology_repository_urls");
      validator.validate(oaiTopologyTaskWithTwoRepositories);

    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("There is more than one repository in input parameters."));
    }
  }

  @Test
  public void shouldValidateTaskForOAITopologyWithMoreThanOneOutputDataset() {
    try {
      DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType("oai_topology_repository_urls");
      validator.validate(oaiTopologyTaskWithTwoOutputDatasets);

    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("There should be exactly one output dataset."));

    }
  }

  @Test
  public void shouldValidateTaskForOAITopologyWithZeroOutputDatasets() {
    try {
      DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType("oai_topology_repository_urls");
      validator.validate(oaiTopologyTaskWithoutOutputDataset);

    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("There should be exactly one output dataset."));

    }
  }

  @Test(expected = DpsTaskValidationException.class)
  public void shouldValidateTasksWithoutHarvestDate() throws DpsTaskValidationException {
    DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType("oai_topology_repository_urls");
    validator.validate(oaiTopologyTaskWithoutHarvestDate);
  }


  @Test
  public void shouldValidateIncrementalTaskForOAITopologyWithSampleSize() {
    try {
      DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType("oai_topology_repository_urls");
      validator.validate(oaiTopologyIncrementalTaskWithSampleSize);

    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("Incremental harvesting could not set SAMPLE_SIZE"));

    }
  }

  @Test
  public void shouldValidateIncrementalTaskForOAITopologyWithoutSampleSize() throws DpsTaskValidationException {
    DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType("oai_topology_repository_urls");
    validator.validate(oaiTopologyIncrementalTaskWithoutSampleSize);
  }
}
