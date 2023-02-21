package eu.europeana.cloud.service.dps;

import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * Object this class store harvested indentifier
 */
@Builder
@Getter
@ToString
public class OAIHeader {

  private String identifier;
}
