package eu.europeana.cloud.service.dps.storm.utils;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;

public class DateHelper {

    public static String getISODateString(Date date) {
        return date.toInstant().toString();
    }

    public static Date parseISODate(String dateString){
        return  Optional.ofNullable(dateString)
                .map(Instant::parse)
                .map(Date::from)
                .orElse(null);
    }
}
