package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.service.utils.validation.custom.FullyDefinedInputRevisionValidator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static eu.europeana.cloud.service.dps.InputDataType.DATASET_URLS;

public class DpsTaskValidatorForMediaTopologyTest {

    private static final String TASK_NAME = "taskName";

    private DpsTask dpsTaskForMediaTopologyWithDataset;
    private DpsTask dpsTaskForMediaTopologyWithoutInputData;
    private DpsTask dpsTaskForMediaTopologyWithoutNewRepresentationParam;
    private DpsTask dpsTaskForMediaTopologyWithoutInputRevision;
    private DpsTask dpsTaskForMediaTopologyWithoutProviderNameInInputRevision;
    private DpsTask dpsTaskForMediaTopologyWithoutRevisionTimestampInInputRevision;
    private DpsTask dpsTaskForMediaTopologyWithoutRevisionNameInInputRevision;


    @Before
    public void init() {
        dpsTaskForMediaTopologyWithDataset = new DpsTask(TASK_NAME);
        final HashMap<InputDataType, List<String>> inputData = new HashMap<>();
        inputData.put(DATASET_URLS, Collections.singletonList("http://test-app1:8080/mcs/data-providers/metis_test5/data-sets/wbc_1"));
        dpsTaskForMediaTopologyWithDataset.addParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME, "sampleName");
        dpsTaskForMediaTopologyWithDataset.setInputData(inputData);
        dpsTaskForMediaTopologyWithDataset.addParameter(PluginParameterKeys.REVISION_PROVIDER, "sampleProvider");
        dpsTaskForMediaTopologyWithDataset.addParameter(PluginParameterKeys.REVISION_NAME, "sampleRevisionName");
        dpsTaskForMediaTopologyWithDataset.addParameter(PluginParameterKeys.REVISION_TIMESTAMP, "2021-03-04T11:02:16.806Z");

        dpsTaskForMediaTopologyWithoutInputData = new DpsTask(TASK_NAME);
        dpsTaskForMediaTopologyWithoutInputData.addParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME, "sampleName");

        dpsTaskForMediaTopologyWithoutNewRepresentationParam = new DpsTask(TASK_NAME);
        inputData.put(DATASET_URLS, Collections.singletonList("http://test-app1:8080/mcs/data-providers/metis_test5/data-sets/wbc_1"));
        dpsTaskForMediaTopologyWithoutNewRepresentationParam.setInputData(inputData);
        //
        dpsTaskForMediaTopologyWithoutInputRevision = new DpsTask(TASK_NAME);
        inputData.put(DATASET_URLS, Collections.singletonList("http://test-app1:8080/mcs/data-providers/metis_test5/data-sets/wbc_1"));
        dpsTaskForMediaTopologyWithoutInputRevision.setInputData(inputData);
        dpsTaskForMediaTopologyWithoutInputRevision.addParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME, "sampleName");
        //
        dpsTaskForMediaTopologyWithoutProviderNameInInputRevision = new DpsTask(TASK_NAME);
        inputData.put(DATASET_URLS, Collections.singletonList("http://test-app1:8080/mcs/data-providers/metis_test5/data-sets/wbc_1"));
        dpsTaskForMediaTopologyWithoutProviderNameInInputRevision.setInputData(inputData);
        dpsTaskForMediaTopologyWithoutProviderNameInInputRevision.addParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME, "sampleName");
        dpsTaskForMediaTopologyWithoutProviderNameInInputRevision.addParameter(PluginParameterKeys.REVISION_NAME, "sampleRevisionName");
        dpsTaskForMediaTopologyWithoutProviderNameInInputRevision.addParameter(PluginParameterKeys.REVISION_TIMESTAMP, "2021-03-04T11:02:16.806Z");
        //
        dpsTaskForMediaTopologyWithoutRevisionTimestampInInputRevision = new DpsTask(TASK_NAME);
        inputData.put(DATASET_URLS, Collections.singletonList("http://test-app1:8080/mcs/data-providers/metis_test5/data-sets/wbc_1"));
        dpsTaskForMediaTopologyWithoutRevisionTimestampInInputRevision.setInputData(inputData);
        dpsTaskForMediaTopologyWithoutRevisionTimestampInInputRevision.addParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME, "sampleName");
        dpsTaskForMediaTopologyWithoutRevisionTimestampInInputRevision.addParameter(PluginParameterKeys.REVISION_NAME, "sampleRevisionName");
        dpsTaskForMediaTopologyWithoutRevisionTimestampInInputRevision.addParameter(PluginParameterKeys.REVISION_TIMESTAMP, "2021-03-04T11:02:16.806Z");
        //
        dpsTaskForMediaTopologyWithoutRevisionNameInInputRevision = new DpsTask(TASK_NAME);
        inputData.put(DATASET_URLS, Collections.singletonList("http://test-app1:8080/mcs/data-providers/metis_test5/data-sets/wbc_1"));
        dpsTaskForMediaTopologyWithoutRevisionNameInInputRevision.setInputData(inputData);
        dpsTaskForMediaTopologyWithoutRevisionNameInInputRevision.addParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME, "sampleName");
        dpsTaskForMediaTopologyWithoutRevisionNameInInputRevision.addParameter(PluginParameterKeys.REVISION_PROVIDER, "sampleRevisionProvider");
        dpsTaskForMediaTopologyWithoutRevisionNameInInputRevision.addParameter(PluginParameterKeys.REVISION_TIMESTAMP, "2021-03-04T11:02:16.806Z");



    }

    @Test
    public void shouldValidateMediaTopologyTask() throws DpsTaskValidationException {
        DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.MEDIA_TOPOLOGY_TASK_WITH_DATASETS);
        validator.validate(dpsTaskForMediaTopologyWithDataset);
    }

    @Test
    public void shouldValidateMediaTopologyTaskWithoutInputData() {
        try {
            DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.MEDIA_TOPOLOGY_TASK_WITH_DATASETS);
            validator.validate(dpsTaskForMediaTopologyWithoutInputData);
            Assert.fail("Should fail on missing DATASET_URL");
        } catch (DpsTaskValidationException e) {
            Assert.assertTrue(e.getMessage().contains("Expected parameter does not exist in dpsTask. Parameter name: DATASET_URLS"));
        }
    }

    @Test
    public void shouldValidateMediaTopologyTaskWithoutInputRevisionDefinedProperly() {
        try {
            DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.MEDIA_TOPOLOGY_TASK_WITH_DATASETS);
            validator.validate(dpsTaskForMediaTopologyWithoutInputRevision);
            Assert.fail("Should fail on FullyDefinedOutputRevisionValidator");
        } catch (DpsTaskValidationException e) {
            Assert.assertTrue("Should fail in proper validator", e.getMessage().contains(FullyDefinedInputRevisionValidator.ERROR_MESSAGE));
        }
    }

    @Test
    public void shouldValidateMediaTopologyTaskWithoutProviderNameInInputRevision() {
        try {
            DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.MEDIA_TOPOLOGY_TASK_WITH_DATASETS);
            validator.validate(dpsTaskForMediaTopologyWithoutProviderNameInInputRevision);
            Assert.fail("Should fail on FullyDefinedOutputRevisionValidator");
        } catch (DpsTaskValidationException e) {
            Assert.assertTrue("Should fail in proper validator", e.getMessage().contains(FullyDefinedInputRevisionValidator.ERROR_MESSAGE));
        }
    }

    @Test
    public void shouldValidateMediaTopologyTaskWithoutInputRevisionTimestamp() {
        try {
            DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.MEDIA_TOPOLOGY_TASK_WITH_DATASETS);
            validator.validate(dpsTaskForMediaTopologyWithoutRevisionTimestampInInputRevision);
            Assert.fail("Should fail on FullyDefinedOutputRevisionValidator");
        } catch (DpsTaskValidationException e) {
            Assert.assertTrue("Should fail in proper validator", e.getMessage().contains(FullyDefinedInputRevisionValidator.ERROR_MESSAGE));
        }
    }

    @Test
    public void shouldValidateMediaTopologyTaskWithoutRevisionNameInInputRevision() {
        try {
            DpsTaskValidator validator = DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.MEDIA_TOPOLOGY_TASK_WITH_DATASETS);
            validator.validate(dpsTaskForMediaTopologyWithoutRevisionNameInInputRevision);
            Assert.fail("Should fail on FullyDefinedOutputRevisionValidator");
        } catch (DpsTaskValidationException e) {
            Assert.assertTrue("Should fail in proper validator", e.getMessage().contains(FullyDefinedInputRevisionValidator.ERROR_MESSAGE));
        }
    }


}
