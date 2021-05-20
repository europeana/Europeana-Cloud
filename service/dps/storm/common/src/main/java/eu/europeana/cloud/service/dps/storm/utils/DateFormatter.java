package eu.europeana.cloud.service.dps.storm.utils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class DateFormatter {

    public static String format(Instant date) {
        return DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.from(ZoneOffset.UTC)).format(date);
    }

    public static Instant parse(String date) {
        return ZonedDateTime.parse(date).toInstant();
    }
}
