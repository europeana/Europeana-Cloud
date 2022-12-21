package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.model.Revision;
import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.With;

@Getter
@With
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class RevisionIdentifier {

  private String revisionName;
  private String revisionProviderId;
  private Date creationTimeStamp;

  public boolean identifies(Revision revision) {
    return revisionName.equals(revision.getRevisionName())
        && revisionProviderId.equals(revision.getRevisionProviderId())
        && ((creationTimeStamp == null) || creationTimeStamp.equals(revision.getCreationTimeStamp()));
  }
}
