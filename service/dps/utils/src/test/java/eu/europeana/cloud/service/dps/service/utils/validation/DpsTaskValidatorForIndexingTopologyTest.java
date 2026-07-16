package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.CreateDpsTaskRequest;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import org.junit.jupiter.api.Test;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.*;
import static eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidatorTest.OUTPUT_PROVIDER;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DpsTaskValidatorForIndexingTopologyTest {

  private static final String FILE_01 = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName-01.txt";
  private static final String FILE_02 = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName-02.txt";
  private static final String FILE_03 = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName-03.txt";
  private static final String DATASET_01_PROVIDER = "metis_test5";
  private static final String DATASET_01_ID = "wbc_1";

  @Test
  void shouldValidateIndexingTopologyTask() throws DpsTaskValidationException {
    CreateDpsTaskRequest dpsTask = prepareDpsTaskForTests(true);
    dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, TargetIndexingDatabase.PREVIEW.toString());
    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_DATASETS);

    validator.validate(dpsTask);
  }

  @Test
  void shouldFailWithBadTargetIndexingDatabaseCase01() {
    CreateDpsTaskRequest dpsTask = prepareDpsTaskForTests(           true);
    dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, "publish");
    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_DATASETS);

    assertThrows(DpsTaskValidationException.class, () -> validator.validate(dpsTask));
  }

  @Test
  void shouldFailWithBadTargetIndexingDatabaseCase02() {
    CreateDpsTaskRequest dpsTask = prepareDpsTaskForTests(true);
    dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, "sample_"+METIS_TARGET_INDEXING_DATABASE);
    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_DATASETS);

    assertThrows(DpsTaskValidationException.class, () -> validator.validate(dpsTask));
  }

  @Test
  void shouldFailWhenNoHarvestDate() {
    CreateDpsTaskRequest dpsTask = prepareDpsTaskForTests(            true);
    dpsTask.removeParameter(HARVEST_DATE);
    dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, TargetIndexingDatabase.PUBLISH.toString());
    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_DATASETS);

    assertThrows(DpsTaskValidationException.class, () -> validator.validate(dpsTask));
  }

  @Test
  void shouldFailsWithoutDataCase01() {
    CreateDpsTaskRequest dpsTask = prepareDpsTaskForTests(false);
    dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, TargetIndexingDatabase.PREVIEW.toString());
    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_DATASETS);

    assertThrows(DpsTaskValidationException.class, () -> validator.validate(dpsTask));
  }

  private CreateDpsTaskRequest prepareDpsTaskForTests(boolean addDatasetUrls) {
    CreateDpsTaskRequest dpsTask = new CreateDpsTaskRequest();

     dpsTask.addParameter(METIS_DATASET_ID,"sample_");
         dpsTask.addParameter(HARVEST_DATE,"sample_"+HARVEST_DATE);



    if (addDatasetUrls) {
      dpsTask.setSource(BatchInfo.builder()
                                 .providerId(DATASET_01_PROVIDER)
                                 .batchId(DATASET_01_ID)
                                 .build());
    }

    dpsTask.setResultsProvider(OUTPUT_PROVIDER);

    return dpsTask;
  }

}
