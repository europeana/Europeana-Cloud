package eu.europeana.cloud.service.mcs.utils;

import static eu.europeana.cloud.common.web.ParamConstants.CLOUD_ID;
import static eu.europeana.cloud.common.web.ParamConstants.FILE_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.REPRESENTATION_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.VERSION;

import java.util.Properties;
import org.junit.Test;

public class EnrichUriUtilTest {

  @Test
  public void testMappingPlaceholdersResolver() {
    EnrichUriUtil.MappingPlaceholdersResolver r = new EnrichUriUtil.MappingPlaceholdersResolver();

    final String CLASS_MAPPING =
        "/records/{" + CLOUD_ID + "}/representations/{" + REPRESENTATION_NAME + "}/versions/{" + VERSION + "}/files/{" + FILE_NAME
            + ":(.+)?}";

    Properties properties = new Properties();
    properties.setProperty(CLOUD_ID, "cloud_id");
    properties.setProperty(REPRESENTATION_NAME, "representation_name");
    properties.setProperty(VERSION, "version");
    properties.setProperty(FILE_NAME, "file_name");

    String s = r.replacePlaceholders(CLASS_MAPPING, properties);
    System.err.println(s);
  }

}
