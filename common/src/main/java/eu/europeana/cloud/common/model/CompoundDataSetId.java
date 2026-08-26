package eu.europeana.cloud.common.model;

import java.util.Set;
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
  private final Set<String> dataSetIds;

  /**
   * Data set's owner (provider) id.
   */
  private final String dataSetProviderId;


  /**
   * Constructs CompoundDataSetId using given provider id and data set id.
   *
   * @param dataSetProviderId provider id
   * @param dataSetIds data sets ids
   */
  public CompoundDataSetId(String dataSetProviderId, Set<String> dataSetIds) {
    this.dataSetIds = dataSetIds;
    this.dataSetProviderId = dataSetProviderId;
  }


  public Set<String> getDataSetIds() {
    return dataSetIds;
  }


  public String getDataSetProviderId() {
    return dataSetProviderId;
  }

  @Override
  public String toString() {
    return "CompoundDataSetId{" + "dataSetIds=" + dataSetIds + ", dataSetProviderId=" + dataSetProviderId + '}';
  }

}
