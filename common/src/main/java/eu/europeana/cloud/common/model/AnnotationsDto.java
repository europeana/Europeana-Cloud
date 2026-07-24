package eu.europeana.cloud.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Wraps annotations delivered do MCS via REST endpoint defined in {@link eu.europeana.cloud.service.mcs.controller.RepresentationVersionAnnotationResource#addAnnotationToRepresentationVersion(String, String, String, AnnotationsDto)}
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AnnotationsDto {

  private RepresentationVersionAnnotation annotation;
}
