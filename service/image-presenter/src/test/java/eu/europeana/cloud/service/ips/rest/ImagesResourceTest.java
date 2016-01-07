package eu.europeana.cloud.service.ips.rest;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.ips.ApplicationContextUtils;
import eu.europeana.cloud.service.ips.ImageTranslator;
import junitparams.JUnitParamsRunner;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.context.ApplicationContext;

import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.InputStream;
import java.net.URI;
import java.util.Scanner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(JUnitParamsRunner.class)
public class ImagesResourceTest extends JerseyTest {

    private ImageTranslator translator;

    static final private String CLOUD_ID = "SDMSJPMRCGZHR5DXPHVJ5AOBECDWLGLV2ZCQOVRJAPHHSBIHA3BA";
    static final private String SCHEMA = "RepName_0000001";
    static final private String VERSION = "81b85300-4faf-11e5-a18d-0a0027000001";
    static final private String FILE_NAME = "01a1fd4c-b79b-490c-8b0d-27f13c2f8c04";
    static final private String FILE_NAME_2 = "abc";
    static final private String MANIFEST_OK = "{\n" +
            "  \"@context\" : \"http://iiif.io/api/image/2/context.json\",\n" +
            "  \"@id\" : \"http://loheria.man.poznan.pl/www-fcgi/iipsrv.fcgi?IIIF=SDMSJPMRCGZHR5DXPHVJ5AOBECDWLGLV2ZCQOVRJAPHHSBIHA3BA_RepName_0000001_81b85300-4faf-11e5-a18d-0a0027000001_01a1fd4c-b79b-490c-8b0d-27f13c2f8c04\",\n" +
            "  \"protocol\" : \"http://iiif.io/api/image\",\n" +
            "  \"width\" : 2974,\n" +
            "  \"height\" : 4120,\n" +
            "  \"tiles\" : [\n" +
            "     { \"width\" : 256, \"height\" : 256, \"scaleFactors\" : [ 1, 2, 4, 8, 16, 32 ] }\n" +
            "  ],\n" +
            "  \"profile\" : [\n" +
            "     \"http://iiif.io/api/image/2/level1.json\",\n" +
            "     { \"formats\" : [ \"jpg\" ],\n" +
            "       \"qualities\" : [ \"native\",\"color\",\"gray\" ],\n" +
            "       \"supports\" : [\"regionByPct\",\"sizeByForcedWh\",\"sizeByWh\",\"sizeAboveFull\",\"rotationBy90s\",\"mirroring\",\"gray\"] }\n" +
            "  ]\n" +
            "}";


    @Override
    public Application configure() {
        return new ResourceConfig().registerClasses(ImagesResource.class)
                .property("contextConfigLocation", "classpath:testContext.xml");
    }


    @Before
    public void mockUp() {
        ApplicationContext applicationContext = ApplicationContextUtils.getApplicationContext();
        translator = applicationContext.getBean(ImageTranslator.class);
        Mockito.reset(translator);
    }


    @Test
    public void getManifestOK()
            throws Exception {
        Mockito.when(translator.getResponse(CLOUD_ID, SCHEMA, VERSION, FILE_NAME + "/")).thenReturn(MANIFEST_OK);

        URI uri = UriBuilder.fromResource(ImagesResource.class).path(ImagesResource.class, "getManifest")
                .buildFromMap(ImmutableMap.<String, String>of(ParamConstants.P_CLOUDID, CLOUD_ID,
                        ParamConstants.P_REPRESENTATIONNAME, SCHEMA, ParamConstants.P_VER, VERSION, ParamConstants.P_FILENAME, FILE_NAME));

        Response response = target(uri.toString()).request().get();

        assertThat(response.getStatus(), is(200));
        assertEquals(MANIFEST_OK, getResponseContent(response));
    }

    private String getResponseContent(Response response) {
        Object entity = response.getEntity();
        if (entity instanceof InputStream) {
            InputStream is = (InputStream) entity;
            Scanner s = new Scanner(is).useDelimiter("\\A");
            try {
                if (s.hasNext())
                    return s.next();
            } finally {
                s.close();
            }
        }
        return null;
    }

    @Test
    public void getManifestReturns404IfFileDoesNotExist()
            throws Exception {
        Mockito.when(translator.getResponse(CLOUD_ID, SCHEMA, VERSION, FILE_NAME_2)).thenReturn(null);

        URI uri = UriBuilder.fromResource(ImagesResource.class).path(ImagesResource.class, "getManifest")
                .buildFromMap(ImmutableMap.<String, String>of(ParamConstants.P_CLOUDID, CLOUD_ID,
                        ParamConstants.P_REPRESENTATIONNAME, SCHEMA, ParamConstants.P_VER, VERSION, ParamConstants.P_FILENAME, FILE_NAME_2));

        Response response = target(uri.toString()).request().get();

        assertThat(response.getStatus(), is(404));
    }


}
