package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.DatasetRevisionInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import java.time.Instant;
import java.util.Date;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidatorTest.prepareCompleteDatasetRevisionInfo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class DpsTaskValidatorForMediaTopologyTest {

    private static final String TASK_NAME = "taskName";

    private DpsTask dpsTaskForMediaTopologyWithDataset;
    private DpsTask dpsTaskForMediaTopologyWithoutInputData;
//    private DpsTask dpsTaskForMediaTopologyWithoutNewRepresentationParam;
//    private DpsTask dpsTaskForMediaTopologyWithoutInputRevision;
//    private DpsTask dpsTaskForMediaTopologyWithoutProviderNameInInputRevision;
//    private DpsTask dpsTaskForMediaTopologyWithoutRevisionTimestampInInputRevision;
//    private DpsTask dpsTaskForMediaTopologyWithoutRevisionNameInInputRevision;


    @BeforeEach
    void init() {
        dpsTaskForMediaTopologyWithDataset = new DpsTask(TASK_NAME);
        dpsTaskForMediaTopologyWithDataset.setOutput(prepareCompleteDatasetRevisionInfo().build());
        dpsTaskForMediaTopologyWithDataset.setInput(prepareCompleteDatasetRevisionInfo().build());

    dpsTaskForMediaTopologyWithoutInputData = new DpsTask(TASK_NAME);
    dpsTaskForMediaTopologyWithoutInputData.setOutput(prepareCompleteDatasetRevisionInfo().build());

//    dpsTaskForMediaTopologyWithoutNewRepresentationParam = new DpsTask(TASK_NAME);
//    inputData.put(DATASET_URLS,
//        Collections.singletonList("http://test-app1:8080/mcs/data-providers/metis_test5/data-sets/wbc_1"));
//    dpsTaskForMediaTopologyWithoutNewRepresentationParam.setInputData(inputData);
//    //
//    dpsTaskForMediaTopologyWithoutInputRevision = new DpsTask(TASK_NAME);
//    inputData.put(DATASET_URLS,
//        Collections.singletonList("http://test-app1:8080/mcs/data-providers/metis_test5/data-sets/wbc_1"));
//    dpsTaskForMediaTopologyWithoutInputRevision.setInputData(inputData);
//    dpsTaskForMediaTopologyWithoutInputRevision.addParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME, "sampleName");
//    //
//    dpsTaskForMediaTopologyWithoutProviderNameInInputRevision = new DpsTask(TASK_NAME);
//    inputData.put(DATASET_URLS,
//        Collections.singletonList("http://test-app1:8080/mcs/data-providers/metis_test5/data-sets/wbc_1"));
//    dpsTaskForMediaTopologyWithoutProviderNameInInputRevision.setInputData(inputData);
//    dpsTaskForMediaTopologyWithoutProviderNameInInputRevision.addParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME,
//        "sampleName");
//    dpsTaskForMediaTopologyWithoutProviderNameInInputRevision.addParameter(PluginParameterKeys.REVISION_NAME,
//        "sampleRevisionName");
//    dpsTaskForMediaTopologyWithoutProviderNameInInputRevision.addParameter(PluginParameterKeys.REVISION_TIMESTAMP,
//        "2021-03-04T11:02:16.806Z");
//    //
//    dpsTaskForMediaTopologyWithoutRevisionTimestampInInputRevision = new DpsTask(TASK_NAME);
//    inputData.put(DATASET_URLS,
//        Collections.singletonList("http://test-app1:8080/mcs/data-providers/metis_test5/data-sets/wbc_1"));
//    dpsTaskForMediaTopologyWithoutRevisionTimestampInInputRevision.setInputData(inputData);
//    dpsTaskForMediaTopologyWithoutRevisionTimestampInInputRevision.addParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME,
//        "sampleName");
//    dpsTaskForMediaTopologyWithoutRevisionTimestampInInputRevision.addParameter(PluginParameterKeys.REVISION_NAME,
//        "sampleRevisionName");
//    dpsTaskForMediaTopologyWithoutRevisionTimestampInInputRevision.addParameter(PluginParameterKeys.REVISION_TIMESTAMP,
//        "2021-03-04T11:02:16.806Z");
//    //
//    dpsTaskForMediaTopologyWithoutRevisionNameInInputRevision = new DpsTask(TASK_NAME);
//    inputData.put(DATASET_URLS,
//        Collections.singletonList("http://test-app1:8080/mcs/data-providers/metis_test5/data-sets/wbc_1"));
//    dpsTaskForMediaTopologyWithoutRevisionNameInInputRevision.setInputData(inputData);
//    dpsTaskForMediaTopologyWithoutRevisionNameInInputRevision.addParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME,
//        "sampleName");
//    dpsTaskForMediaTopologyWithoutRevisionNameInInputRevision.addParameter(PluginParameterKeys.REVISION_PROVIDER,
//        "sampleRevisionProvider");
//    dpsTaskForMediaTopologyWithoutRevisionNameInInputRevision.addParameter(PluginParameterKeys.REVISION_TIMESTAMP,
//        "2021-03-04T11:02:16.806Z");


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

//    @Test
//    void shouldValidateMediaTopologyTaskWithoutInputRevisionDefinedProperly() {
//        try {
//            DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType(
//                    DpsTaskValidatorFactory.MEDIA_TOPOLOGY_TASK_WITH_DATASETS);
//            validator.validate(dpsTaskForMediaTopologyWithoutInputRevision);
//            fail("Should fail on FullyDefinedOutputRevisionValidator");
//        } catch (DpsTaskValidationException e) {
//            assertTrue(e.getMessage().contains(FullyDefinedMCSInputValidator.ERROR_MESSAGE), "Should fail in proper validator");
//        }
//    }

//    @Test
//    void shouldValidateMediaTopologyTaskWithoutProviderNameInInputRevision() {
//        try {
//            DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType(
//                    DpsTaskValidatorFactory.MEDIA_TOPOLOGY_TASK_WITH_DATASETS);
//            validator.validate(dpsTaskForMediaTopologyWithoutProviderNameInInputRevision);
//            fail("Should fail on FullyDefinedOutputRevisionValidator");
//        } catch (DpsTaskValidationException e) {
//            assertTrue(e.getMessage().contains(FullyDefinedMCSInputValidator.ERROR_MESSAGE),
//                    "Should fail in proper validator");
//        }
//    }

//    @Test
//    void shouldValidateMediaTopologyTaskWithoutInputRevisionTimestamp() {
//        try {
//            DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType(
//                    DpsTaskValidatorFactory.MEDIA_TOPOLOGY_TASK_WITH_DATASETS);
//            validator.validate(dpsTaskForMediaTopologyWithoutRevisionTimestampInInputRevision);
//            fail("Should fail on FullyDefinedOutputRevisionValidator");
//        } catch (DpsTaskValidationException e) {
//            assertTrue(e.getMessage().contains(FullyDefinedMCSInputValidator.ERROR_MESSAGE),
//                    "Should fail in proper validator");
//        }
//    }

//    @Test
//    void shouldValidateMediaTopologyTaskWithoutRevisionNameInInputRevision() {
//        try {
//            DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType(
//                    DpsTaskValidatorFactory.MEDIA_TOPOLOGY_TASK_WITH_DATASETS);
//            validator.validate(dpsTaskForMediaTopologyWithoutRevisionNameInInputRevision);
//            fail("Should fail on FullyDefinedOutputRevisionValidator");
//        } catch (DpsTaskValidationException e) {
//            assertTrue(e.getMessage().contains(FullyDefinedMCSInputValidator.ERROR_MESSAGE),
//                    "Should fail in proper validator");
//        }
//    }


}
