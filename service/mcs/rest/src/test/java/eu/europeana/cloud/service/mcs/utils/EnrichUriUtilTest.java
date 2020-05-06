package eu.europeana.cloud.service.mcs.utils;

import org.junit.Test;

import java.util.Properties;

import static eu.europeana.cloud.common.web.ParamConstants.*;

public class EnrichUriUtilTest {

    @Test
    public void testMappingPlaceholdersResolver() {
        EnrichUriUtil.MappingPlaceholdersResolver r = new EnrichUriUtil.MappingPlaceholdersResolver();

        final String CLASS_MAPPING =
                "/records/{"+CLOUD_ID+"}/representations/{"+REPRESENTATION_NAME+"}/versions/{"+VERSION+"}/files/{"+FILE_NAME+":(.+)?}";

        Properties properties = new Properties();
        properties.setProperty(CLOUD_ID, "cloud_id");
        properties.setProperty(REPRESENTATION_NAME, "representation_name");
        properties.setProperty(VERSION, "version");
        properties.setProperty(FILE_NAME, "file_name");

        String s = r.replacePlaceholders(CLASS_MAPPING, properties);
        System.err.println(s);
    }

}
