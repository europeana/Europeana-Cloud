package eu.europeana.cloud.service.dps;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class DpsRecord implements Serializable {

  private static final long serialVersionUID = 1L;

  private long taskId;
  private String recordId;
  private String metadataPrefix;
  private boolean markedAsDeleted;
}
