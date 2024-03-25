package eu.europeana.cloud.service.dps;

import com.google.common.base.Objects;
import jakarta.xml.bind.annotation.XmlRootElement;

import java.io.Serializable;
import java.util.Date;

@XmlRootElement
public class OAIPMHHarvestingDetails implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Schemas to harvest - optional
   */
  private String schema;

  /**
   * Set to harvest - optional
   */
  private String set;

  /**
   * From date - optional
   */
  private Date dateFrom;

  /**
   * Until date - optional
   */
  private Date dateUntil;

  /**
   * dates granularity supported by the source
   */
  private String granularity;

  public OAIPMHHarvestingDetails() {
    // serialization purposes
  }

  public OAIPMHHarvestingDetails(String schema) {
    this.schema = schema;
  }

  public OAIPMHHarvestingDetails(String schema, String set, Date dateFrom, Date dateUntil, String granularity) {
    this.schema = schema;
    this.set = set;
    this.dateFrom = dateFrom;
    this.dateUntil = dateUntil;
    this.granularity = granularity;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public String getSet() {
    return set;
  }

  public void setSet(String set) {
    this.set = set;
  }

  public Date getDateFrom() {
    return dateFrom;
  }

  public void setDateFrom(Date dateFrom) {
    this.dateFrom = dateFrom;
  }

  public Date getDateUntil() {
    return dateUntil;
  }

  public void setDateUntil(Date dateUntil) {
    this.dateUntil = dateUntil;
  }

  public String getGranularity() {
    return granularity;
  }

  public void setGranularity(String granularity) {
    this.granularity = granularity;
  }

  @Override
  @SuppressWarnings({"squid:S1067"})
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }
    OAIPMHHarvestingDetails that = (OAIPMHHarvestingDetails) o;
    return Objects.equal(schema, that.schema) &&
        Objects.equal(set, that.set) &&
        Objects.equal(dateFrom, that.dateFrom) &&
        Objects.equal(dateUntil, that.dateUntil);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(schema, set, dateFrom, dateUntil, granularity);
  }

  @Override
  public String toString() {
    return "OAIPMHHarvestingDetails{" +
        "schema=" + schema +
        ", set=" + set +
        ", dateFrom=" + dateFrom +
        ", dateUntil=" + dateUntil +
        ", granularity=" + granularity +
        '}';
  }


}
