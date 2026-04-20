package eu.europeana.cloud.common.model.dps;

//TODO
//Directly copied from metis core with modification for task.
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
public class EcloudTaskProgress {

    // The total number of expected records excluding deleted records.
    private int expectedRecords;
    // The total number of records processed so far excluding deleted records and including ignored records if applicable.
    private int successRecords;
    // The total number of records processed that resulted in at least one report containing WARN type message
    private int warningRecords;
    // The number of records containing errors encountered so far.
    private int failRecords;
    // The number of duplicated records processed so far.
    private int duplicateRecords;
    // The number of processed records so far that are to be ignored for follow-up tasks.
    // Mostly unchanged records recognized during OAI/HTTP HARVEST
    private int unchangedRecords;
    // Total number of processed records so far.
    // Should be equal to successRecords + failRecords + unchangedRecords + duplicateRecords
    private int processedRecords;
    // Total number of post processed records so far
    private int postProcessedRecords;
    // The number of post processed records.
    private int expectedPostProcessedRecords;
    // The number of depublish records.
    private int expectedDepublishRecords;
    // The number of successfully depublished records so far.
    private int successDepublishRecords;
    // The number of unsuccessfully depublished records so far.
    private int failDepublishRecords;
    // The number of depublished records so far.
    private int processedDepublishRecords;




    // The current state of the task.
    private EngineTaskState engineTaskState;

    private String engineTaskStateInfo;

    public int getDuplicateRecords() {
        return duplicateRecords;
    }

    public void setDuplicateRecords(int duplicateRecords) {
        this.duplicateRecords = duplicateRecords;
    }

    public int getWarningRecords() {
        return warningRecords;
    }

    public void setWarningRecords(int warningRecords) {
        this.warningRecords = warningRecords;
    }

    public int getExpectedDepublishRecords() {
        return expectedDepublishRecords;
    }

    public void setExpectedDepublishRecords(int expectedDepublishRecords) {
        this.expectedDepublishRecords = expectedDepublishRecords;
    }

    public int getFailDepublishRecords() {
        return failDepublishRecords;
    }

    public void setFailDepublishRecords(int failDepublishRecords) {
        this.failDepublishRecords = failDepublishRecords;
    }

    public int getExpectedRecords() {
        return expectedRecords;
    }

    public void setExpectedRecords(int expectedRecords) {
        this.expectedRecords = expectedRecords;
    }

    public int getSuccessRecords() {
        return successRecords;
    }

    public void setSuccessRecords(int successRecords) {
        this.successRecords = successRecords;
    }

    public int getUnchangedRecords() {
        return unchangedRecords;
    }

    public void setUnchangedRecords(int unchangedRecords) {
        this.unchangedRecords = unchangedRecords;
    }

    public int getSuccessDepublishRecords() {
        return successDepublishRecords;
    }

    public void setSuccessDepublishRecords(int successDepublishRecords) {
        this.successDepublishRecords = successDepublishRecords;
    }

    public int getFailRecords() {
        return failRecords;
    }

    public void setFailRecords(int failRecords) {
        this.failRecords = failRecords;
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

    public int getPostProcessedRecords() {
        return postProcessedRecords;
    }

    public void setPostProcessedRecords(int postProcessedRecords) {
        this.postProcessedRecords = postProcessedRecords;
    }
}