package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.DpsTask;
import java.util.function.Predicate;

public abstract class CustomValidator implements Predicate<DpsTask> {

  public String errorMessage() {
    return "[" + this.getClass().getSimpleName() + "]. " + detailedMessage();
  }

  public abstract String detailedMessage();
}
