package eu.europeana.cloud.service.dps.metis.indexing;

import eu.europeana.cloud.common.utils.DateAdapter;
import java.io.Serializable;
import java.util.Date;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@Data
@Builder
@NoArgsConstructor
public class MetisDataSetParameters implements Serializable {

  private static final long serialVersionUID = 123456789L;

  private String dataSetId;
  private TargetIndexingDatabase targetIndexingDatabase;

  @XmlElement(name = "cleaningDate", required = true)
  @XmlJavaTypeAdapter(DateAdapter.class)
  private Date cleaningDate;

  public MetisDataSetParameters(String dataSetId, TargetIndexingDatabase targetIndexingDatabase, Date cleaningDate) {
    this.dataSetId = dataSetId;
    this.targetIndexingDatabase = targetIndexingDatabase;
    this.cleaningDate = cleaningDate;
  }
}
