package eu.europeana.cloud.service.dps.storm.incremental;

import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import lombok.Builder;
import lombok.Getter;

/**
 * Describes result of the categorization process
 */
@Builder
@Getter
public class CategorizationResult {

  private final Category category;
  private final CategorizationParameters categorizationParameters;
  private final HarvestedRecord harvestedRecord;

  public boolean shouldBeProcessed() {
    return category.equals(Category.ELIGIBLE_FOR_PROCESSING);
  }

  public boolean shouldBeDropped() {
    return category.equals(Category.ALREADY_PROCESSED);
  }

  public enum Category {
    ELIGIBLE_FOR_PROCESSING,
    ALREADY_PROCESSED
  }
}
