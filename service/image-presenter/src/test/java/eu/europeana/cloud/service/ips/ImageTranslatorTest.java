package eu.europeana.cloud.service.ips;

import org.junit.Test;

import static org.junit.Assert.assertNull;

/**
 * Created by helin on 2015-10-16.
 */
public class ImageTranslatorTest {

    static final private String CLOUD_ID = "SDMSJPMRCGZHR5DXPHVJ5AOBECDWLGLV2ZCQOVRJAPHHSBIHA3BA";
    static final private String SCHEMA = "RepName_0000001";
    static final private String VERSION = "81b85300-4faf-11e5-a18d-0a0027000001";
    static final private String FILE_NAME = "01a1fd4c-b79b-490c-8b0d-27f13c2f8c04";
    static final private String IIP_SERVER = "http://loheria.man.poznan.pl/www-fcgi/iipsrv.fcgi";

    @Test
    public void nullResponseWnenNoIIPServer()
    {
        ImageTranslator it = new ImageTranslator(null);

        String response = it.getResponse(CLOUD_ID, SCHEMA, VERSION, FILE_NAME);
        assertNull(response);
    }


    @Test
    public void nullResponseWnenNullParameters()
    {
        ImageTranslator it = new ImageTranslator(IIP_SERVER);

        String response = it.getResponse(null, SCHEMA, VERSION, FILE_NAME);
        assertNull(response);
    }
}
