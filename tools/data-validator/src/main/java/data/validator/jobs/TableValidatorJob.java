package data.validator.jobs;


import data.validator.DataValidator;

import java.util.concurrent.Callable;

/**
 * Created by Tarek on 4/28/2017.
 */

public class TableValidatorJob implements Callable<Void> {
    private DataValidator dataValidator;
    private String sourceTableName;
    private String targetTableName;
    private int threadsCount;

    public TableValidatorJob(DataValidator dataValidator, String sourceTableName, String targetTableName, int threadsCount) {
        this.dataValidator = dataValidator;
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.threadsCount = threadsCount;
    }

    @Override
    public Void call() throws Exception {
        dataValidator.validate(sourceTableName, targetTableName, threadsCount);
        return null;
    }


}
