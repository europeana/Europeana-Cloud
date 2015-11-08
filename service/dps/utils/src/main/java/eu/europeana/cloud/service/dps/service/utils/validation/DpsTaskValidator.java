package eu.europeana.cloud.service.dps.service.utils.validation;

import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.dps.DpsTask;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DpsTaskValidator {

    private List<DpsTaskConstraint> dpsTaskConstraints = new ArrayList<>();

    public DpsTaskValidator withParameter(String parameterName) {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.PARAMETER, parameterName);

        dpsTaskConstraints.add(constraint);
        return this;

    }

    /**
     * Will check if dps task contains parameter with selected name and selected value
     *
     * @param parameterName
     * @param parameterValue
     * @return
     */
    public DpsTaskValidator withParameter(String parameterName, String parameterValue) {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.PARAMETER, parameterName, parameterValue);

        dpsTaskConstraints.add(constraint);
        return this;
    }

    /**
     * Will check if dps task contains parameter with selected name (value of this parameter will not be validated)
     *
     * @param parameterName
     * @return
     */
    public DpsTaskValidator withEmptyParameter(String parameterName) {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.PARAMETER, parameterName, "");

        dpsTaskConstraints.add(constraint);
        return this;
    }

    /**
     * Will check if dps task contains input data with selected name (value of this input data will not be validated)
     *
      * @param inputDataName
     * @return
     */
    public DpsTaskValidator withDataEntry(String inputDataName) {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.INPUT_DATA, inputDataName);
        dpsTaskConstraints.add(constraint);
        return this;
    }

    /**
     * Will check if dps task contains input data with selected name and selected value
     *
     * @param entryName
     * @param entryValue
     * @return
     */
    public DpsTaskValidator withDataEntry(String entryName, Object entryValue) {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.INPUT_DATA, entryName, entryValue);
        dpsTaskConstraints.add(constraint);
        return this;
    }

    /**
     * Will check if dps task contains input data with selected name and selected content type
     *
     * @param entryName
     * @param contentType content type of input data entry (can be file url, dataset url, ...)
     * @return
     */
    public DpsTaskValidator withDataEntry(String entryName, InputDataValueType contentType) {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.INPUT_DATA, entryName, contentType);
        dpsTaskConstraints.add(constraint);
        return this;
    }

    /**
     * Will check if dps task contains selected name
     *
     * @param taskName
     * @return
     */
    public DpsTaskValidator withName(String taskName) {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.NAME, null, taskName);
        dpsTaskConstraints.add(constraint);
        return this;
    }

    /**
     * Will check if dps task contains any name
     *
     * @return
     */
    public DpsTaskValidator withAnyName() {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.NAME);
        dpsTaskConstraints.add(constraint);
        return this;
    }

    /**
     * Will check if dps task contains selected task id
     * @param taskId
     * @return
     */
    public DpsTaskValidator withId(long taskId) {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.ID, null, taskId + "");
        dpsTaskConstraints.add(constraint);
        return this;
    }
    /**
     * Will check if dps task contains any task id
     *
     * @return
     */
    public DpsTaskValidator withAnyId() {
        DpsTaskConstraint constraint = new DpsTaskConstraint(DpsTaskFieldType.ID);
        dpsTaskConstraints.add(constraint);
        return this;
    }

    /**
     * Validates dps task according to constraints list
     *
     * @param task task to be validated
     *
     * @throws DpsTaskValidationException
     */
    public void validate(DpsTask task) throws DpsTaskValidationException {
        int constraintsNumber = dpsTaskConstraints.size();

        for (DpsTaskConstraint re : dpsTaskConstraints) {
            DpsTaskFieldType fieldType = re.getFieldType();
            if (fieldType.equals(DpsTaskFieldType.NAME)) {
                validateName(task, re);
            } else if (fieldType.equals(DpsTaskFieldType.PARAMETER)) {
                validateParameter(task, re);
            } else if (fieldType.equals(DpsTaskFieldType.INPUT_DATA)) {
                validateInputData(task, re);
            } else if (fieldType.equals(DpsTaskFieldType.ID)) {
                validateId(task, re);
            }
        }
    }

    /*
     * Nazwa:
     * 1.Bez nazwy;         : pomijamy ten warunek (?)
     * 2. Z dowolną nazwą   : null
     * 3. Z nazwą pustą     : ""
     * 4. Z ustaloną nazwą  : "ustalona nazwa"
     */

    private void validateName(DpsTask task, DpsTaskConstraint constraint) throws DpsTaskValidationException {
        String taskName = task.getTaskName();
        if (constraint.getExpectedValue() == null && taskName != null) {  //any name
            return;
        }
        if ("".equals(constraint.getExpectedValue()) && "".equals(taskName)) {//empty name
            return;
        }
        if (constraint.getExpectedValue().equals(taskName)) {//exact name
            return;
        }

        throw new DpsTaskValidationException("Task name is not valid.");
    }

    /*
     * Parameter:
     * 1. Brak parametrów;         : pomijamy ten warunek (?)
     * 2. Jest parametr o podanej nazwie z dowolną wartością : parameterName, null expected value
     * 3. Jest parametr o padanej nazwie z pustą wartością: parameterName, expected value ''
     * 4. Jest parametr o padanej nazwie i podanej wartości: parameterName, expected value 'avlja'
     */
    private void validateParameter(DpsTask task, DpsTaskConstraint constraint) throws DpsTaskValidationException {
        String expectedParameter = task.getParameter(constraint.getExpectedName());
        if (expectedParameter == null) {
            throw new DpsTaskValidationException("Expected parameter does not exist in dpsTask. Parameter name: " + constraint.getExpectedName());
        }
        if (constraint.getExpectedValue() == null && expectedParameter != null) {  //any name
            return;
        }
        if ("".equals(constraint.getExpectedValue()) && "".equals(expectedParameter)) {  //empty value
            return;
        }
        if (expectedParameter.equals(constraint.getExpectedValue())) {
            return;
        }
        throw new DpsTaskValidationException("Parameter does not meet constraints. Parameter name: " + constraint.getExpectedName());
    }

    /**
     * 1. Brak inputData;         : pomijamy ten warunek (?)
     * 2. Jest inputData o podanej nazwie z dowolną wartością : parameterName, null expected value
     * 3. Jest inputData o padanej nazwie z pustą wartością: parameterName, expected value ''
     * 4. Jest inputData o padanej nazwie i podanej wartości: parameterName, expected value 'lista z jakimiś wartościami'
     * 5. Jest inputData o padanej nazwie i podanej zawartości (np. linki do plików): ???
     *
     * @param task
     * @param constraint
     * @throws DpsTaskValidationException
     */
    private void validateInputData(DpsTask task, DpsTaskConstraint constraint) throws DpsTaskValidationException {
        List<String> expectedInputData = task.getDataEntry(constraint.getExpectedName());

        if (expectedInputData == null) {
            throw new DpsTaskValidationException("Expected parameter does not exist in dpsTask. Parameter name: " + constraint.getExpectedName());
        }
        if (constraint.getExpectedValueType() != null) {
            for (String expectedInputDataValue : expectedInputData) {
                if(constraint.getExpectedValueType().equals(InputDataValueType.LINK_TO_FILE)){
                    try {
                        UrlParser parser = new UrlParser(expectedInputDataValue);
                        if(parser.isUrlToRepresentationVersionFile()){
                            continue;
                        }
                        throw new DpsTaskValidationException("Wrong input data: " + expectedInputDataValue);
                    } catch (MalformedURLException e) {
                        throw new DpsTaskValidationException("Wrong input data: " + expectedInputDataValue);
                    }
                }else if(constraint.getExpectedValueType().equals(InputDataValueType.LINK_TO_DATASET)){
                    try {
                        UrlParser parser = new UrlParser(expectedInputDataValue);
                        if(parser.isUrlToDataset()){
                            continue;
                        }
                        throw new DpsTaskValidationException("Wrong input data: " + expectedInputDataValue);
                    } catch (MalformedURLException e) {
                        throw new DpsTaskValidationException("Wrong input data: " + expectedInputDataValue);
                    }
                }
            }
        }
        if (constraint.getExpectedValue() == null && expectedInputData != null) {   //any value
            return;
        }
        if ("".equals(constraint.getExpectedValue()) && expectedInputData.size() == 0) {    //empty value
            return;
        }
        if (expectedInputData.equals(constraint.getExpectedValue())) {  //exact value
            return;
        }
        throw new DpsTaskValidationException("Input data is not valid.");

    }

    /**
     * Id:
     * 1. Brak id;         : pomijamy ten warunek (?)
     * * 2. Z dowolnym id : null
     * 3. Z pustym id  : to nie działa tutaj bo id jest long'iem czyli zawsze będzie miało jakąś wartość
     * 4. Z ustalonym id : "ustalone id" (podajemy)
     */
    private void validateId(DpsTask task, DpsTaskConstraint constraint) throws DpsTaskValidationException {
        long taskId = task.getTaskId();
        if (constraint.getExpectedValue() == null) {  //any id
            return;
        }
        if (constraint.getExpectedValue().equals(taskId + "")) {//exacted id
            return;
        }
        throw new DpsTaskValidationException("Task id is not valid.");
    }
}

class DpsTaskConstraint {
    private DpsTaskFieldType fieldType;
    private Object expectedValue;
    private InputDataValueType expectedValueType;
    private String expectedName;
    private int expectedAmount = -1;


    public DpsTaskConstraint(DpsTaskFieldType fieldType, Object expectedValue, int expectedAmount) {//amount nie pasuje tutaj
        this.expectedValue = expectedValue;
        this.fieldType = fieldType;
        this.expectedAmount = expectedAmount;
    }

    public DpsTaskConstraint(DpsTaskFieldType fieldType, String expectedName, Object expectedValue) {
        this.fieldType = fieldType;
        this.expectedName = expectedName;
        this.expectedValue = expectedValue;
    }

    public DpsTaskConstraint(DpsTaskFieldType fieldType, String expectedName, InputDataValueType expectedValueType) {
        this.fieldType = fieldType;
        this.expectedName = expectedName;
        this.expectedValueType = expectedValueType;
    }

    public DpsTaskConstraint(DpsTaskFieldType fieldType, String expectedName) {
        this.fieldType = fieldType;
        this.expectedName = expectedName;
    }

    public DpsTaskConstraint(DpsTaskFieldType fieldType) {
        this.fieldType = fieldType;
    }

    public Object getExpectedValue() {
        return expectedValue;
    }

    public InputDataValueType getExpectedValueType() {
        return expectedValueType;
    }

    public DpsTaskFieldType getFieldType() {
        return fieldType;
    }

    public String getExpectedName() {
        return expectedName;
    }
}

enum DpsTaskFieldType {
    PARAMETER,
    INPUT_DATA,
    ID,
    NAME;
}