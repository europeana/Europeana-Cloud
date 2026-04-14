package eu.europeana.cloud.common.model.dps;

//TODO
//Directly copied from metis core.
//Should be moved from metis core to metis common in order to have one common class for all processing engines
//For development purposes only
//NOTE: we kinda need those annotations - so in end solution we either add those to child class inheriting from this class in metis common
//or we add this directly into metis common, or we just add equal method and those constructors
//NOTE2: expectedPostProcessedRecordsNumber is missing from there? Is that intentional?
//NOTE3: postProcessedRecordsCount got suffix Count in it, it is not aligned with other variable names

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Contains execution progress information of a task.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class EngineTaskProgress {

    // The total number of expected records excluding deleted records.
    private int expectedRecords;

    // The total number of records processed so far excluding deleted records and including ignored records if applicable.
    private int processedRecords;

    // The number of processed records so far that are to be ignored for follow-up tasks.
    private int ignoredRecords = 0;

    // The number of deleted records processed so far.
    private int deletedRecords = 0;

    // The number of post processed records.
    private int postProcessedRecordsCount = 0;

    // The number of errors encountered so far.
    private int processedErrors;

    private int deletedErrors;

    // The current state of the task.
    private EngineTaskState engineTaskState;

    private String engineTaskStateInfo;

    public int getExpectedRecords() {
        return expectedRecords;
    }

    public void setExpectedRecords(int expectedRecords) {
        this.expectedRecords = expectedRecords;
    }

    public int getProcessedRecords() {
        return processedRecords;
    }

    public void setProcessedRecords(int processedRecords) {
        this.processedRecords = processedRecords;
    }

    public int getIgnoredRecords() {
        return ignoredRecords;
    }

    public void setIgnoredRecords(int ignoredRecords) {
        this.ignoredRecords = ignoredRecords;
    }

    public int getDeletedRecords() {
        return deletedRecords;
    }

    public void setDeletedRecords(int deletedRecords) {
        this.deletedRecords = deletedRecords;
    }

    public int getProcessedErrors() {
        return processedErrors;
    }

    public void setProcessedErrors(int processedErrors) {
        this.processedErrors = processedErrors;
    }

    public int getDeletedErrors() {
        return deletedErrors;
    }

    public void setDeletedErrors(int deletedErrors) {
        this.deletedErrors = deletedErrors;
    }

    public EngineTaskState getEngineTaskState() {
        return engineTaskState;
    }

    public void setEngineTaskState(EngineTaskState engineTaskState) {
        this.engineTaskState = engineTaskState;
    }

    public String getEngineTaskStateInfo() {
        return engineTaskStateInfo;
    }

    public void setEngineTaskStateInfo(String engineTaskStateInfo) {
        this.engineTaskStateInfo = engineTaskStateInfo;
    }

    public int getPostProcessedRecordsCount() {
        return postProcessedRecordsCount;
    }

    public void setPostProcessedRecordsCount(int postProcessedRecordsCount) {
        this.postProcessedRecordsCount = postProcessedRecordsCount;
    }
}