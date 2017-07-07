package eu.europeana.cloud.service.dps.service.utils;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidator;
import eu.europeana.cloud.service.dps.service.utils.validation.InputDataValueType;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static eu.europeana.cloud.service.dps.InputDataType.DATASET_URLS;
import static eu.europeana.cloud.service.dps.InputDataType.FILE_URLS;

public class DpsTaskValidatorTest {

    private static final long TASK_ID = 121212;
    private DpsTask dpsTask;
    private DpsTask icTopologyTask;
    private DpsTask dpsTaskWithIncorrectRevision_1;
    private DpsTask dpsTaskWithIncorrectRevision_2;
    private DpsTask dpsTaskWithIncorrectRevision_3;
    private DpsTask dpsTaskWithIncorrectRevision_4;
    private DpsTask dpsTaskWithIncorrectRevision_5;
    private DpsTask dpsTaskWithIncorrectRevision_6;
    private DpsTask dpsTaskWithIncorrectRevision_7;
    private DpsTask dpsTaskWithIncorrectRevision_8;
    private DpsTask dpsTaskWithIncorrectRevision_9;

    private static final String TASK_NAME = "taksName";
    private static final String EXISTING_PARAMETER_NAME = "param_1";
    private static final String EXISTING_PARAMETER_VALUE = "param_1_value";
    private static final String EMPTY_PARAMETER_NAME = "empty_param";

    private static final InputDataType EXISTING_DATA_ENTRY_NAME = DATASET_URLS;
    private static final List<String> EXISTING_DATA_ENTRY_VALUE = Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt");

    private static final Revision correctRevision = new Revision("sampleRevisionName", "sampleRevisionProvider");

    @Before
    public void init() {
        dpsTask = new DpsTask();
        dpsTask.setTaskName(TASK_NAME);
        dpsTask.addParameter(EXISTING_PARAMETER_NAME, EXISTING_PARAMETER_VALUE);
        dpsTask.addParameter(EMPTY_PARAMETER_NAME, "");
        dpsTask.addDataEntry(EXISTING_DATA_ENTRY_NAME, EXISTING_DATA_ENTRY_VALUE);
        dpsTask.setOutputRevision(correctRevision);
        //
        icTopologyTask = new DpsTask();
        icTopologyTask.addDataEntry(FILE_URLS, Arrays.asList("http://iks-kbase.synat.pcss.pl:9090/mcs/records/JP46FLZLVI2UYV4JNHTPPAB4DGPESPY4SY4N5IUQK4SFWMQ3NUQQ/representations/tiff/versions/74c56880-7733-11e5-b38f-525400ea6731/files/f59753a5-6d75-4d48-9f4d-4690b671240c", "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        icTopologyTask.addParameter("OUTPUT_MIME_TYPE", "image/jp2");
        icTopologyTask.addParameter("SAMPLE_PARAMETER", "sampleParameterValue");
        //
        dpsTaskWithIncorrectRevision_1 = new DpsTask(TASK_NAME);
        Revision r1 = new Revision();
        dpsTaskWithIncorrectRevision_1.setOutputRevision(r1);
        //
        dpsTaskWithIncorrectRevision_2 = new DpsTask(TASK_NAME);
        Revision revisionWithoutProviderId = new Revision();
        revisionWithoutProviderId.setRevisionName("sampleRevisionName");
        dpsTaskWithIncorrectRevision_2.setOutputRevision(revisionWithoutProviderId);
        //
        dpsTaskWithIncorrectRevision_3 = new DpsTask(TASK_NAME);
        Revision revisionWithoutName = new Revision();
        revisionWithoutName.setRevisionProviderId("sampleRevisionProvider");
        dpsTaskWithIncorrectRevision_3.setOutputRevision(revisionWithoutName);
        //
        dpsTaskWithIncorrectRevision_4 = new DpsTask(TASK_NAME);
        Revision revisionWithEmptyName = new Revision();
        revisionWithEmptyName.setRevisionProviderId("sampleRevisionProvider");
        revisionWithEmptyName.setRevisionName("");
        dpsTaskWithIncorrectRevision_4.setOutputRevision(revisionWithEmptyName);
        //
        dpsTaskWithIncorrectRevision_5 = new DpsTask(TASK_NAME);
        Revision revisionWithEmptyProviderId = new Revision();
        revisionWithEmptyProviderId.setRevisionProviderId("");
        revisionWithEmptyProviderId.setRevisionName("sampleRevisionName");
        dpsTaskWithIncorrectRevision_5.setOutputRevision(revisionWithEmptyProviderId);
        //
        dpsTaskWithIncorrectRevision_6 = new DpsTask(TASK_NAME);
        Revision revisionWithEmptyProviderIdAndName = new Revision();
        revisionWithEmptyProviderIdAndName.setRevisionProviderId("");
        revisionWithEmptyProviderIdAndName.setRevisionName("");
        dpsTaskWithIncorrectRevision_6.setOutputRevision(revisionWithEmptyProviderIdAndName);
        //
        dpsTaskWithIncorrectRevision_7 = new DpsTask(TASK_NAME);
        Revision revisionWithEmptyProviderId_1 = new Revision();
        revisionWithEmptyProviderId_1.setRevisionProviderId("  ");
        revisionWithEmptyProviderId_1.setRevisionName("sampleRevisionName");
        dpsTaskWithIncorrectRevision_7.setOutputRevision(revisionWithEmptyProviderId_1);
        //
        dpsTaskWithIncorrectRevision_8 = new DpsTask(TASK_NAME);
        Revision revisionWithEmptyName_1 = new Revision();
        revisionWithEmptyName_1.setRevisionProviderId("sampleProviderId");
        revisionWithEmptyName_1.setRevisionName(" ");
        dpsTaskWithIncorrectRevision_8.setOutputRevision(revisionWithEmptyName_1);
        //
        dpsTaskWithIncorrectRevision_9 = new DpsTask(TASK_NAME);
        Revision revisionWithEmptyProviderIdAndName_1 = new Revision();
        revisionWithEmptyProviderIdAndName_1.setRevisionProviderId(" ");
        revisionWithEmptyProviderIdAndName_1.setRevisionName("  ");
        dpsTaskWithIncorrectRevision_9.setOutputRevision(revisionWithEmptyProviderIdAndName_1);
    }

    @Test
    public void shouldValidateThatTaskIsCorrectWhenConstraintsListIsEmpty() throws DpsTaskValidationException {
        new DpsTaskValidator().validate(dpsTask);
    }

    ////
    // Task id
    ////
    @Test
    public void anyIdShouldBeValidated() throws DpsTaskValidationException {
        new DpsTaskValidator().withAnyId().validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void shouldThrowExceptionForWrongTaskId() throws DpsTaskValidationException {
        new DpsTaskValidator().withId(-1).validate(dpsTask);
    }

    ////
    //Task name
    ////
    @Test
    public void validatorShouldValidateThatTaskHasSelectedName() throws DpsTaskValidationException {
        new DpsTaskValidator().withName(TASK_NAME).validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatTaskHasWrongName() throws DpsTaskValidationException {
        new DpsTaskValidator().withName("someWrongName").validate(dpsTask);
    }

    @Test
    public void validatorShouldValidateThatTaskHasAnyName() throws DpsTaskValidationException {
        new DpsTaskValidator().withAnyName().validate(dpsTask);
    }

    ////
    //Parameters
    ////
    @Test
    public void validatorShouldValidateThatThereIsParameterWithSelectedNameAndAnyValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withParameter(EXISTING_PARAMETER_NAME).validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatThereIsNoParameterWithSelectedName() throws DpsTaskValidationException {
        new DpsTaskValidator().withParameter("p2").validate(dpsTask);
    }

    @Test
    public void validatorShouldValidateThatThereIsSelectedParameterWithCorrectValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withParameter(EXISTING_PARAMETER_NAME, EXISTING_PARAMETER_VALUE).validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatThereIsSelectedParameterWithWrongValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withParameter("p1", "p3").validate(dpsTask);
    }

    @Test
    public void validatorShouldValidateThatThereIsSelectedParameterWithEmptyValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withEmptyParameter(EMPTY_PARAMETER_NAME).validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatThereIsNoSelectedParameterWithEmptyValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withEmptyParameter("nonEmptyParameter").validate(dpsTask);
    }


    ////
    //inputData
    ////
    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatThereIsNoInputDataWithSelectedName() throws DpsTaskValidationException {
        new DpsTaskValidator().withDataEntry("notExistingDataEntry").validate(dpsTask);
    }

    @Test
    public void validatorShouldValidateThatThereIsInputDataWithSelectedNameAndAnyValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withDataEntry(EXISTING_DATA_ENTRY_NAME.name()).validate(dpsTask);
    }

    @Test
    public void validatorShouldValidateThatThereIsInputDataWithSelectedNameAndCorrectValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withDataEntry(EXISTING_DATA_ENTRY_NAME.name(), Arrays.asList
                ("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt")).validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatThereIsInputDataWithSelectedNameAndWrongValue() throws DpsTaskValidationException {
        new DpsTaskValidator().withDataEntry(EXISTING_DATA_ENTRY_NAME.name(), Arrays.asList("someWrongValue"))
                .validate(dpsTask);
    }

    @Test
    public void validatorShouldValidateThatThereIsInputDataWithSelectedNameAndCorrectContentType() throws DpsTaskValidationException {
        new DpsTaskValidator().withDataEntry(EXISTING_DATA_ENTRY_NAME.name(), InputDataValueType.LINK_TO_FILE)
                .validate(dpsTask);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatThereIsInputDataWithSelectedNameAndIncorrectContentType() throws DpsTaskValidationException {
        new DpsTaskValidator().withDataEntry(EXISTING_DATA_ENTRY_NAME.name(), InputDataValueType
                .LINK_TO_DATASET).validate(dpsTask);
    }

    @Test
    public void shouldValidateTaskForICTopology() throws DpsTaskValidationException {
        new DpsTaskValidator()
                .withDataEntry("FILE_URLS", InputDataValueType.LINK_TO_FILE)
                .withParameter("OUTPUT_MIME_TYPE")
                .withParameter("SAMPLE_PARAMETER")
                .validate(icTopologyTask);
    }


    ////
    //Output Revision
    ////
    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatOutputRevisionIsNotCorrect_1() throws DpsTaskValidationException {
        new DpsTaskValidator().validate(dpsTaskWithIncorrectRevision_1);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatOutputRevisionIsNotCorrect_2() throws DpsTaskValidationException {
        new DpsTaskValidator().validate(dpsTaskWithIncorrectRevision_2);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatOutputRevisionIsNotCorrect_3() throws DpsTaskValidationException {
        new DpsTaskValidator().validate(dpsTaskWithIncorrectRevision_3);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatOutputRevisionIsNotCorrect_4() throws DpsTaskValidationException {
        new DpsTaskValidator().validate(dpsTaskWithIncorrectRevision_4);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatOutputRevisionIsNotCorrect_5() throws DpsTaskValidationException {
        new DpsTaskValidator().validate(dpsTaskWithIncorrectRevision_5);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatOutputRevisionIsNotCorrect_6() throws DpsTaskValidationException {
        new DpsTaskValidator().validate(dpsTaskWithIncorrectRevision_6);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatOutputRevisionIsNotCorrect_7() throws DpsTaskValidationException {
        new DpsTaskValidator().validate(dpsTaskWithIncorrectRevision_7);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatOutputRevisionIsNotCorrect_8() throws DpsTaskValidationException {
        new DpsTaskValidator().validate(dpsTaskWithIncorrectRevision_8);
    }

    @Test(expected = DpsTaskValidationException.class)
    public void validatorShouldValidateThatOutputRevisionIsNotCorrect_9() throws DpsTaskValidationException {
        new DpsTaskValidator().validate(dpsTaskWithIncorrectRevision_9);
    }

    @Test
    public void validatorShouldValidateDpsTaskWithCorrectOutputRevision() throws DpsTaskValidationException {
        new DpsTaskValidator().validate(dpsTask);
    }

}
