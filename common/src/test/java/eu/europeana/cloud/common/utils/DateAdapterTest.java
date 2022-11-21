package eu.europeana.cloud.common.utils;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class DateAdapterTest {

  //example string representing date that is serialized to xml/json. It is number of ms from 1970
  public static final String DATE_STRING = "1613480338321";

  //DATE reprezented by field DATE_STRING
  public static final Date DATE = new Date(1613480338321L);

  private static DateAdapter dateAdapter;

  @BeforeClass
  public static void init() {
    dateAdapter = new DateAdapter();
  }

  @Test
  public void shouldSerializeTheDateSuccessfully() {
    Date date = dateAdapter.unmarshal(DATE_STRING);
    assertEquals(DATE, date);
  }

  @Test
  public void shouldDeSerializeTheDateSuccessfully() {
    assertEquals(dateAdapter.marshal(DATE), DATE_STRING);
  }

  @Test(expected = NumberFormatException.class)
  public void shouldThrowParsingException() {
    String unParsedDateString = "2017-11-23";
    dateAdapter.unmarshal(unParsedDateString);
  }

  @Test
  public void shouldCreateNullDateInCaseEmptyOrNull() {
    assertNull(dateAdapter.unmarshal(null));
    assertNull(dateAdapter.unmarshal(""));
  }

  @Test(expected = RuntimeException.class)
  public void shouldThrowRunTimeException() {
    dateAdapter.marshal(null);
  }

}