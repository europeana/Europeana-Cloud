package eu.europeana.cloud.service.dps.metis.indexing;

import com.google.common.base.Objects;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Date;

@XmlRootElement()
public class DataSetCleanerParameters {
    private static final long serialVersionUID = 123456789L;

    private String dataSetId;
    private String isUsingALtEnv;
    private String targetIndexingEnv;
    private Date cleaningDate;

    public DataSetCleanerParameters() {
    }

    public String getDataSetId() {
        return dataSetId;
    }

    public void setDataSetId(String dataSetId) {
        this.dataSetId = dataSetId;
    }

    public String getIsUsingALtEnv() {
        return isUsingALtEnv;
    }

    public void setIsUsingALtEnv(String isUsingALtEnv) {
        this.isUsingALtEnv = isUsingALtEnv;
    }

    public String getTargetIndexingEnv() {
        return targetIndexingEnv;
    }

    public void setTargetIndexingEnv(String targetIndexingEnv) {
        this.targetIndexingEnv = targetIndexingEnv;
    }

    public Date getCleaningDate() {
        return cleaningDate;
    }

    public void setCleaningDate(Date cleaningDate) {
        this.cleaningDate = cleaningDate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DataSetCleanerParameters)) {
            return false;
        }
        DataSetCleanerParameters cleanerParameters = (DataSetCleanerParameters) o;

        return Objects.equal(dataSetId, cleanerParameters.dataSetId) &&
                Objects.equal(isUsingALtEnv, cleanerParameters.isUsingALtEnv) &&
                Objects.equal(targetIndexingEnv, cleanerParameters.targetIndexingEnv) &&
                Objects.equal(cleaningDate, cleanerParameters.cleaningDate);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dataSetId, isUsingALtEnv, targetIndexingEnv, cleaningDate);
    }

}
