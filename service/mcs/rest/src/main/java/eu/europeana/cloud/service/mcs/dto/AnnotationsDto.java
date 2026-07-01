package eu.europeana.cloud.service.mcs.dto;

import eu.europeana.cloud.common.model.RepresentationVersionAnnotation;
import lombok.Data;

/**
 * Wraps annotations delivered do MCS via REST endpoint defined in {@link eu.europeana.cloud.service.mcs.controller.RepresentationVersionAnnotationResource#addAnnotationToRepresentationVersion(String, String, String, AnnotationsDto)}
 */
@Data
public class AnnotationsDto {

  private RepresentationVersionAnnotation annotation;
}
