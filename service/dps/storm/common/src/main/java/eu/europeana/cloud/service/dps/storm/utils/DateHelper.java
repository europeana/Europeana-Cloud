package eu.europeana.cloud.service.dps.storm.utils;

import org.apache.commons.lang3.time.FastDateFormat;

import java.util.Date;
import java.util.TimeZone;

public class DateHelper {
    private static final String DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";

    public static String getUTCDateString(Date date) {
        FastDateFormat formatter = FastDateFormat.getInstance(DATE_FORMAT, TimeZone.getTimeZone("UTC"));
        return formatter.format(date);
    }
}
