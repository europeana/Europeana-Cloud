package eu.europeana.cloud.service.dps.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * Created by Tarek on 6/2/2016.
 */
public class DateUtil {
    public static Date parseSentTime(String sentTime) {
        Date date = null;
        if (sentTime != null) {
            try {
                SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd kk:mm:ss z yyyy", Locale.ENGLISH);
                date = formatter.parse(sentTime);
            } catch (ParseException ex) {
                ex.printStackTrace();
            }
        }
        return date;
    }
}
