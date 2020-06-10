package eu.europeana.cloud.service.dps.metis.indexing;

import com.google.common.base.Objects;
import eu.europeana.cloud.common.utils.DateAdapter;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.io.Serializable;
import java.util.Date;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@Getter
@Setter
@ToString
public class DataSetCleanerParameters implements Serializable {
    private static final long serialVersionUID = 123456789L;

    private String dataSetId;
    private boolean usingAltEnv;
    private String targetIndexingEnv;

    @XmlElement(name = "cleaningDate", required = true)
    @XmlJavaTypeAdapter(DateAdapter.class)
    private Date cleaningDate;

    public DataSetCleanerParameters(String dataSetId, boolean usingAltEnv, String targetIndexingEnv, Date cleaningDate) {
        this.dataSetId = dataSetId;
        this.usingAltEnv = usingAltEnv;
        this.targetIndexingEnv = targetIndexingEnv;
        this.cleaningDate = cleaningDate;
    }


    public DataSetCleanerParameters() {
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
                usingAltEnv == cleanerParameters.usingAltEnv &&
                Objects.equal(targetIndexingEnv, cleanerParameters.targetIndexingEnv) &&
                Objects.equal(cleaningDate, cleanerParameters.cleaningDate);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(dataSetId, usingAltEnv, targetIndexingEnv, cleaningDate);
    }

}
