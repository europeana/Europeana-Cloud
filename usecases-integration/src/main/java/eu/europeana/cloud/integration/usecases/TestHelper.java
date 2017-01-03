package eu.europeana.cloud.integration.usecases;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by Tarek on 12/7/2016.
 */
public class TestHelper {
    private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    //2016-10-05 10:05:05+0200
    public final static String getTime() {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.format(date);
    }
}
