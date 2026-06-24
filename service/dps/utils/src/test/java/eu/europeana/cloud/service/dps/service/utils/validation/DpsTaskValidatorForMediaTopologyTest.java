package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidatorTest.prepareCompleteDatasetRevisionInfo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DpsTaskValidatorForMediaTopologyTest {

    private static final String TASK_NAME = "taskName";

    private DpsTask dpsTaskForMediaTopologyWithDataset;
    private DpsTask dpsTaskForMediaTopologyWithoutInputData;
//    private DpsTask dpsTaskForMediaTopologyWithoutNewRepresentationParam;


    @BeforeEach
    void init() {
        dpsTaskForMediaTopologyWithDataset = new DpsTask(TASK_NAME);
        dpsTaskForMediaTopologyWithDataset.setOutput(prepareCompleteDatasetRevisionInfo().build());
        dpsTaskForMediaTopologyWithDataset.setInput(prepareCompleteDatasetRevisionInfo().build());

    dpsTaskForMediaTopologyWithoutInputData = new DpsTask(TASK_NAME);
    dpsTaskForMediaTopologyWithoutInputData.setOutput(prepareCompleteDatasetRevisionInfo().build());
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
