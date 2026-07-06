package eu.europeana.cloud.service.dps;

import jakarta.xml.bind.annotation.XmlRootElement;

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

/**
 * Definition of the OAI harvesting DpsTask input
 */
@XmlRootElement
@Data
@AllArgsConstructor
@Builder
public class OAIPMHHarvestingDetails implements TaskInput, Serializable {

  @Serial
  private static final long serialVersionUID = 1L;

  private String repositoryUrl;

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

  /**
   * Creates OAIPMHHarvestingDetails
   * @param schema - OAI metadata form
   */
  public OAIPMHHarvestingDetails(String schema) {
    this.schema = schema;
  }

  /**
   * Creates OAIPMHHarvestingDetails
   * @param schema - OAI metadata form
   * @param set - OAI source dataset name
   * @param dateFrom - optional start datestamp
   * @param dateUntil - optional end datestamp
   * @param granularity - timestamp granularity
   */
  public OAIPMHHarvestingDetails(String schema, String set, Date dateFrom, Date dateUntil, String granularity) {
    this.schema = schema;
    this.set = set;
    this.dateFrom = dateFrom;
    this.dateUntil = dateUntil;
    this.granularity = granularity;
  }

}
