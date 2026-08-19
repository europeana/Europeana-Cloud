package eu.europeana.cloud.service.mcs.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import eu.europeana.cloud.common.model.Annotation;
import lombok.Data;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Wraps annotations delivered do MCS via REST endpoint defined in {@link eu.europeana.cloud.service.mcs.controller.RepresentationVersionAnnotationResource#addAnnotationToRepresentationVersion(String, String, String, AnnotationsDto)}
 */
@Data
public class AnnotationsDto {

  private Map<String, String> annotations;

  @JsonIgnore
  public Set<Annotation> getValues() {
    HashSet<Annotation> result = new HashSet<>();

    annotations.keySet().forEach(key -> result.add(new Annotation(Annotation.AnnotationKey.valueOf(key), annotations.get(key))));
    return result;
  }
}
