package eu.europeana.cloud.service.commons.utils;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Optional;

public final class DateHelper {

  private DateHelper() {
  }

  public static String getISODateString(Date date) {
    return Optional.ofNullable(date)
                   .map(Date::toInstant)
                   .map(Instant::toString)
                   .orElse(null);
  }

  public static Date parseISODate(String dateString) {
    return Optional.ofNullable(dateString)
                   .map(Instant::parse)
                   .map(Date::from)
                   .orElse(null);
  }

  public static String format(Instant date) {
    return DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.from(ZoneOffset.UTC)).format(date);
  }

  public static String format(Date date) {
    return format(date.toInstant());
  }

  public static Instant parse(String date) {
    return ZonedDateTime.parse(date).toInstant();
  }
}
