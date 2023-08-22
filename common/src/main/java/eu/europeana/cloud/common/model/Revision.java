package eu.europeana.cloud.common.model;

import eu.europeana.cloud.common.utils.DateAdapter;
import eu.europeana.cloud.common.utils.Tags;
import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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

  public Revision(String revisionName, String providerId) {
    this(revisionName, providerId, new Date(), false);
  }

  public Revision(String revisionName, String providerId, Date creationTimeStamp) {
    this(revisionName, providerId, creationTimeStamp, false);
  }

  public Revision(final Revision revision) {
    this(revision.getRevisionName(),
        revision.getRevisionProviderId(),
        revision.getCreationTimeStamp(),
        revision.isDeleted()
    );
  }

  public static Revision fromParams(String revisionName, String revisionProviderId, String tag) {
    Revision revision = new Revision(revisionName, revisionProviderId);
    revision.setRevisionTags(revision, new HashSet<>(List.of(tag)));
    return revision;
  }

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
