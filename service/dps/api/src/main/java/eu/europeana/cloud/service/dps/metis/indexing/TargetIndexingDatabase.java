package eu.europeana.cloud.service.dps.metis.indexing;

import eu.europeana.cloud.common.model.RepresentationVersionAnnotation;
import eu.europeana.cloud.common.model.RepresentationVersionAnnotation.AnnotationKey;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;

public enum TargetIndexingDatabase {
  PUBLISH(AnnotationKey.PUBLISHED),
  PREVIEW(AnnotationKey.PREVIEWING);
  private static List<String> values = initializeTargetIndexingDatabaseValues();

  @Getter
  private final AnnotationKey annotationKey;

  TargetIndexingDatabase(AnnotationKey annotationKey) {
    this.annotationKey = annotationKey;
  }

  public static List<String> getTargetIndexingDatabaseValues() {
    return values;
  }

  private static List<String> initializeTargetIndexingDatabaseValues() {
    List<String> values = new ArrayList<>(TargetIndexingDatabase.values().length);
    for (TargetIndexingDatabase targetIndexingDatabase : TargetIndexingDatabase.values()) {
      values.add(targetIndexingDatabase.toString());
    }
    return values;
  }
}

