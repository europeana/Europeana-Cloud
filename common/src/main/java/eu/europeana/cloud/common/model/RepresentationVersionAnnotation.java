package eu.europeana.cloud.common.model;

import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Annotation for {@link Representation}
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@XmlRootElement(name = "annotation")
public class RepresentationVersionAnnotation {

  private AnnotationKey key;
  private String value;

  /**
   * All possible annotation keys to be used
   */
  public enum AnnotationKey {
    VALID, INVALID, PUBLISHED, PREVIEWING
  }
}

