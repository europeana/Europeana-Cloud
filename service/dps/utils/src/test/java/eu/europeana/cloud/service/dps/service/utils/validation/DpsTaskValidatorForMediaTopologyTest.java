package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.CreateDpsTaskRequest;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidatorTest.OUTPUT_PROVIDER;
import static eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidatorTest.prepareCompleteDatasetRevisionInfo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DpsTaskValidatorForMediaTopologyTest {

    private static final String TASK_NAME = "taskName";

    private CreateDpsTaskRequest dpsTaskForMediaTopologyWithDataset;
    private CreateDpsTaskRequest dpsTaskForMediaTopologyWithoutInputData;
//    private CreateDpsTaskRequest dpsTaskForMediaTopologyWithoutNewRepresentationParam;


    @BeforeEach
    void init() {
      dpsTaskForMediaTopologyWithDataset = new CreateDpsTaskRequest();
      dpsTaskForMediaTopologyWithDataset.setResultsProvider(OUTPUT_PROVIDER);
      dpsTaskForMediaTopologyWithDataset.setSource(prepareCompleteDatasetRevisionInfo().build());

      dpsTaskForMediaTopologyWithoutInputData = new CreateDpsTaskRequest();
      dpsTaskForMediaTopologyWithoutInputData.setResultsProvider(OUTPUT_PROVIDER);
    }

    @Test
    void shouldValidateMediaTopologyTask() throws DpsTaskValidationException {
        DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType(
                DpsTaskValidatorFactory.MEDIA_TOPOLOGY_TASK_WITH_DATASETS);
        validator.validate(dpsTaskForMediaTopologyWithDataset);
    }

    @Test
    void shouldValidateMediaTopologyTaskWithoutInputData() {
      DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType(
          DpsTaskValidatorFactory.MEDIA_TOPOLOGY_TASK_WITH_DATASETS);
      assertThrows(DpsTaskValidationException.class,
          () -> validator.validate(dpsTaskForMediaTopologyWithoutInputData));
    }

}
