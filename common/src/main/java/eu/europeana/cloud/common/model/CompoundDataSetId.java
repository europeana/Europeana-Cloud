package eu.europeana.cloud.common.model;

import lombok.EqualsAndHashCode;

/**
 * Globally unique data set id. Data set id is unique only for a certain provider id, so the combination of those two identifiers
 * is globally unique.
 */
@EqualsAndHashCode
public class CompoundDataSetId {

  /**
   * Data set id (unique for provider).
   */
  private final String dataSetId;

  /**
   * Data set's owner (provider) id.
   */
  private final String dataSetProviderId;


  /**
   * Constructs CompoundDataSetId using given provider id and data set id.
   *
   * @param dataSetProviderId provider id
   * @param dataSetId data set id
   */
  public CompoundDataSetId(String dataSetProviderId, String dataSetId) {
    this.dataSetId = dataSetId;
    this.dataSetProviderId = dataSetProviderId;
  }


  public String getDataSetId() {
    return dataSetId;
  }


  public String getDataSetProviderId() {
    return dataSetProviderId;
  }

  @Override
  public String toString() {
    return "CompoundDataSetId{" + "dataSetId=" + dataSetId + ", dataSetProviderId=" + dataSetProviderId + '}';
  }

}
