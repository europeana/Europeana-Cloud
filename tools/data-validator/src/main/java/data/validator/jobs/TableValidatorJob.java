package data.validator.jobs;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import data.validator.DataValidator;

import java.util.List;
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
