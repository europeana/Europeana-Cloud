package eu.europeana.cloud.service.dps.properties;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class GeneralProperties {

  private String fileServerLocationInternal;
  private String fileServerLocationExternal;
  private String mcsLocation;
  private String uisLocation;
}
