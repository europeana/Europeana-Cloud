package eu.europeana.cloud.service.dps.service.utils.validation.custom;

import eu.europeana.cloud.service.dps.CreateDpsTaskRequest;
import java.util.function.Predicate;

public abstract class CustomValidator implements Predicate<CreateDpsTaskRequest> {

  public String errorMessage() {
    return "[" + this.getClass().getSimpleName() + "]. " + detailedMessage();
  }

  public abstract String detailedMessage();
}
