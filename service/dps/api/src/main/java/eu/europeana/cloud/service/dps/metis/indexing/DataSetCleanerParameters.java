package eu.europeana.cloud.service.dps.metis.indexing;

import com.google.common.base.Objects;
import eu.europeana.cloud.common.utils.DateAdapter;
import java.io.Serializable;
import java.util.Date;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@Getter
@Setter
@ToString
@NoArgsConstructor
public class DataSetCleanerParameters implements Serializable {

  private static final long serialVersionUID = 123456789L;

  private String dataSetId;
  private String targetIndexingEnv;

  @XmlElement(name = "cleaningDate", required = true)
  @XmlJavaTypeAdapter(DateAdapter.class)
  private Date cleaningDate;

  public DataSetCleanerParameters(String dataSetId, String targetIndexingEnv, Date cleaningDate) {
    this.dataSetId = dataSetId;
    this.targetIndexingEnv = targetIndexingEnv;
    this.cleaningDate = cleaningDate;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    DataSetCleanerParameters cleanerParameters = (DataSetCleanerParameters) o;

    return Objects.equal(dataSetId, cleanerParameters.dataSetId) &&
        Objects.equal(targetIndexingEnv, cleanerParameters.targetIndexingEnv) &&
        Objects.equal(cleaningDate, cleanerParameters.cleaningDate);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(dataSetId, targetIndexingEnv, cleaningDate);
  }

}
