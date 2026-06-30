package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import org.junit.jupiter.api.Test;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.*;
import static eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidatorTest.prepareCompleteDatasetRevisionInfo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DpsTaskValidatorForIndexingTopologyTest {

  private static final String TASK_NAME = "taskName";
  private static final String FILE_01 = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName-01.txt";
  private static final String FILE_02 = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName-02.txt";
  private static final String FILE_03 = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName-03.txt";
  private static final String DATASET_01_PROVIDER = "metis_test5";
  private static final String DATASET_01_ID = "wbc_1";

  @Test
  void shouldValidateIndexingTopologyTask() throws DpsTaskValidationException {
    DpsTask dpsTask = prepareDpsTaskForTests(true);
    dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, TargetIndexingDatabase.PREVIEW.toString());
    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_DATASETS);

    validator.validate(dpsTask);
  }

  @Test
  void shouldFailWithBadTargetIndexingDatabaseCase01() {
    DpsTask dpsTask = prepareDpsTaskForTests(           true);
    dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, "publish");
    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_DATASETS);

    assertThrows(DpsTaskValidationException.class, () -> validator.validate(dpsTask));
  }

  @Test
  void shouldFailWithBadTargetIndexingDatabaseCase02() {
    DpsTask dpsTask = prepareDpsTaskForTests(true);
    dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, "sample_"+METIS_TARGET_INDEXING_DATABASE);
    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_DATASETS);

    assertThrows(DpsTaskValidationException.class, () -> validator.validate(dpsTask));
  }

  @Test
  void shouldFailWhenNoHarvestDate() {
    DpsTask dpsTask = prepareDpsTaskForTests(            true);
    dpsTask.removeParameter(HARVEST_DATE);
    dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, TargetIndexingDatabase.PUBLISH.toString());
    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_DATASETS);

    assertThrows(DpsTaskValidationException.class, () -> validator.validate(dpsTask));
  }

  @Test
  void shouldFailsWithoutDataCase01() {
    DpsTask dpsTask = prepareDpsTaskForTests(false);
    dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, TargetIndexingDatabase.PREVIEW.toString());
    DpsTaskValidator validator =
            DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_DATASETS);

    assertThrows(DpsTaskValidationException.class, () -> validator.validate(dpsTask));
  }

  private DpsTask prepareDpsTaskForTests(boolean addDatasetUrls) {
    DpsTask dpsTask = new DpsTask(TASK_NAME);

     dpsTask.addParameter(METIS_DATASET_ID,"sample_");
         dpsTask.addParameter(HARVEST_DATE,"sample_"+HARVEST_DATE);



    if (addDatasetUrls) {
      dpsTask.setInput(BatchInfo.builder()
                                .providerId(DATASET_01_PROVIDER)
                                .batchId(DATASET_01_ID)
                                .representationName("sample_REPRESENTATION_NAME")
                                .build());
    }

    dpsTask.setOutput(prepareCompleteDatasetRevisionInfo().build());

    return dpsTask;
  }

}
