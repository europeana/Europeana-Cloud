package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static eu.europeana.cloud.service.dps.InputDataType.DATASET_URLS;
import static eu.europeana.cloud.service.dps.InputDataType.FILE_URLS;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.*;

public class DpsTaskValidatorForIndexingTopologyTest {

    private static final String TASK_NAME = "taskName";
    private static final String FILE_01 = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName-01.txt";
    private static final String FILE_02 = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName-02.txt";
    private static final String FILE_03 = "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName-03.txt";
    private static final String DATASET_01 = "http://test-app1:8080/mcs/data-providers/metis_test5/data-sets/wbc_1";

    @Test
    public void shouldValidateIndexingTopologyTask() throws DpsTaskValidationException {
        DpsTask dpsTask = prepareDpsTaskForTests(
                Arrays.asList(REPRESENTATION_NAME, METIS_DATASET_ID, HARVEST_DATE, REVISION_NAME, REVISION_PROVIDER, REVISION_TIMESTAMP),
                true,
                false
        );

        dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, TargetIndexingDatabase.PREVIEW.toString());

        DpsTaskValidator validator =
                DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_DATASETS);
        validator.validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void shouldFailWithBadTargetIndexingDatabaseCase01() throws DpsTaskValidationException {
        DpsTask dpsTask = prepareDpsTaskForTests(
                Arrays.asList(REPRESENTATION_NAME, METIS_DATASET_ID, HARVEST_DATE, REVISION_NAME, REVISION_PROVIDER, REVISION_TIMESTAMP),
                true,
                false
        );

        dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, "publish");

        DpsTaskValidator validator =
                DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_DATASETS);
        validator.validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void shouldFailWithBadTargetIndexingDatabaseCase02() throws DpsTaskValidationException {
        DpsTask dpsTask = prepareDpsTaskForTests(
                Arrays.asList(METIS_TARGET_INDEXING_DATABASE, REPRESENTATION_NAME, METIS_DATASET_ID, HARVEST_DATE, REVISION_NAME, REVISION_PROVIDER, REVISION_TIMESTAMP),
                true,
                false
        );

        DpsTaskValidator validator =
                DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_DATASETS);
        validator.validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void shouldFailWhenNoHarvestDate() throws DpsTaskValidationException {
        DpsTask dpsTask = prepareDpsTaskForTests(
                Arrays.asList(REPRESENTATION_NAME, METIS_DATASET_ID, REVISION_NAME, REVISION_PROVIDER, REVISION_TIMESTAMP),
                true,
                false
        );

        dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, TargetIndexingDatabase.PUBLISH.toString());

        DpsTaskValidator validator =
                DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_DATASETS);
        validator.validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void shouldFailWithoutRevisionData() throws DpsTaskValidationException {
        DpsTask dpsTask = prepareDpsTaskForTests(
                Arrays.asList(REPRESENTATION_NAME, METIS_DATASET_ID, HARVEST_DATE),
                true,
                false
        );

        dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, TargetIndexingDatabase.PREVIEW.toString());

        DpsTaskValidator validator =
                DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_DATASETS);
        validator.validate(dpsTask);
    }

    @Test
    public void shouldValidateIndexingTopologyTaskWithFilesUrls() throws DpsTaskValidationException {
        DpsTask dpsTask = prepareDpsTaskForTests(
                Arrays.asList(REPRESENTATION_NAME, METIS_DATASET_ID, HARVEST_DATE, REVISION_NAME, REVISION_PROVIDER, REVISION_TIMESTAMP),
                false,
                true
        );

        dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, TargetIndexingDatabase.PREVIEW.toString());

        DpsTaskValidator validator =
                DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_FILE_URLS);
        validator.validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void shouldFailsWithoutDataCase01() throws DpsTaskValidationException {
        DpsTask dpsTask = prepareDpsTaskForTests(
                Arrays.asList(REPRESENTATION_NAME, METIS_DATASET_ID, HARVEST_DATE, REVISION_NAME, REVISION_PROVIDER, REVISION_TIMESTAMP),
                false,
                false
        );

        dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, TargetIndexingDatabase.PREVIEW.toString());

        DpsTaskValidator validator =
                DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_DATASETS);
        validator.validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void shouldFailsWithoutDataCase02() throws DpsTaskValidationException {
        DpsTask dpsTask = prepareDpsTaskForTests(
                Arrays.asList(REPRESENTATION_NAME, METIS_DATASET_ID, HARVEST_DATE, REVISION_NAME, REVISION_PROVIDER, REVISION_TIMESTAMP),
                false,
                false
        );

        dpsTask.addParameter(METIS_TARGET_INDEXING_DATABASE, TargetIndexingDatabase.PREVIEW.toString());

        DpsTaskValidator validator =
                DpsTaskValidatorFactory.createValidatorForTaskType(DpsTaskValidatorFactory.INDEXING_TOPOLOGY_TASK_WITH_FILE_URLS);
        validator.validate(dpsTask);
    }


    private DpsTask prepareDpsTaskForTests(List<String> parameters, boolean addDatasetUrls, boolean addFilesUrls) {
        DpsTask dpsTask = new DpsTask(TASK_NAME);
        parameters.forEach(parameter -> dpsTask.addParameter(parameter, "sample_"+parameter));

        if(addDatasetUrls) {
            final HashMap<InputDataType, List<String>> inputData = new HashMap<>();
            inputData.put(DATASET_URLS, Collections.singletonList(DATASET_01));
            dpsTask.setInputData(inputData);
        } else if(addFilesUrls) {
            final HashMap<InputDataType, List<String>> inputData = new HashMap<>();
            inputData.put(FILE_URLS, Arrays.asList(FILE_01, FILE_02, FILE_03));
            dpsTask.setInputData(inputData);
        }

        return dpsTask;
    }

}
