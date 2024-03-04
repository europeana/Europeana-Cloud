package eu.europeana.cloud.common.model;

import eu.europeana.cloud.common.utils.DateAdapter;
import eu.europeana.cloud.common.utils.Tags;
import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * <p>This class describes revision object used in representation versions</p>
 * <p>Revision consists for four parts:
 * <li>name</li>
 * <li>provider (owner of the revision)</li>
 * <li>creation timestamp</li>
 * <li>boolean flag indicating if the revision is marked as deleted</li>
 *
 * </p>
 *
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Revision implements Serializable {

  private static final long serialVersionUID = 1L;

  private String revisionName;
  private String revisionProviderId;

  @XmlElement(name = "creationTimeStamp", required = true)
  @XmlJavaTypeAdapter(DateAdapter.class)
  private Date creationTimeStamp;

  boolean deleted;

  /**
   * Creates new instance of the {@link Revision} class based on provided value
   *
   * @param revisionName revision name
   * @param providerId  revision provider
   */
  public Revision(String revisionName, String providerId) {
    this(revisionName, providerId, new Date(), false);
  }

  /**
   * Creates new instance of the {@link Revision} class based on provided value
   *
   * @param revisionName revision name
   * @param providerId  revision provider
   * @param creationTimeStamp revision timestamp
   */
  public Revision(String revisionName, String providerId, Date creationTimeStamp) {
    this(revisionName, providerId, creationTimeStamp, false);
  }

  /**
   * Creates new instance of the {@link Revision} class based on provided value
   *
   * @param revision {@link Revision} instance that will be used to construct new one
   */
  public Revision(final Revision revision) {
    this(revision.getRevisionName(),
        revision.getRevisionProviderId(),
        revision.getCreationTimeStamp(),
        revision.isDeleted()
    );
  }

  /**
   * Creates new instance of the {@link Revision} class based on provided value
   *
   * @param revisionName revision name
   * @param revisionProviderId revision provider
   * @param tag revision tag (see {@link Tags})
   *
   * @return new instance of {@link Revision} class
   */
  public static Revision fromParams(String revisionName, String revisionProviderId, String tag) {
    Revision revision = new Revision(revisionName, revisionProviderId);
    revision.setRevisionTags(revision, new HashSet<>(List.of(tag)));
    return revision;
  }

  /**
   * Creates new instance of the {@link Revision} class based on provided value
   *
   * @param revisionName revision name
   * @param revisionProviderId revision provider
   * @param tags list of tags (see {@link Tags})
   *
   * @return new instance of {@link Revision} class
   */
  public static Revision fromParams(String revisionName, String revisionProviderId, Set<String> tags) {
    Revision revision = new Revision(revisionName, revisionProviderId);
    revision.setRevisionTags(revision, tags);
    return revision;
  }

  private void setRevisionTags(Revision revision, Set<String> tags) {
    if (tags == null || tags.isEmpty()) {
      return;
    }
    if (tags.contains(Tags.DELETED.getTag())) {
      revision.setDeleted(true);
    }
  }

}
