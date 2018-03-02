package eu.europeana.cloud.common.utils;

import org.junit.BeforeClass;
import org.junit.Test;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by Tarek on 11/30/2017.
 */
public class DateAdapterTest {
    private static int YEAR = 2017;
    private static int MONTH = 11;
    private static int DAY = 23;
    private static int HOUR = 10;
    private static int MINUTE = 43;
    private static int SECOND = 26;
    private static int MILLISECOND = 38;
    public static final String DATE_STRING = YEAR + "-" + MONTH + "-" + DAY + "T" + HOUR + ":" + MINUTE + ":" + SECOND + ".0" + MILLISECOND + "Z";// "2017-11-23T10:43:26.038Z";
    private static DateAdapter dateAdapter;
    private static Calendar cal;

    @BeforeClass
    public static void init() {
        dateAdapter = new DateAdapter();
        cal = prepareCalender();
    }

    @Test
    public void shouldSerializeTheDateSuccessfully() throws ParseException {
        Date date = dateAdapter.unmarshal(DATE_STRING);
        assertEquals(cal.getTime(), date);
    }

    @Test
    public void shouldDeSerializeTheDateSuccessfully() {
        assertEquals(dateAdapter.marshal(cal.getTime()), DATE_STRING);
    }

    @Test(expected = ParseException.class)
    public void shouldThrowParsingException() throws ParseException {
        String unParsedDateString = "2017-11-23";
        dateAdapter.unmarshal(unParsedDateString);
    }

    @Test
    public void shouldCreateNullDateInCaseEmptyOrNull() throws ParseException {
        assertNull(dateAdapter.unmarshal(null));
        assertNull(dateAdapter.unmarshal(""));
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowRunTimeException() {
        dateAdapter.marshal(null);
    }

    private static Calendar prepareCalender() {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone((TimeZone.getTimeZone("UTC")));
        calendar.set(Calendar.YEAR, YEAR);
        calendar.set(Calendar.MONTH, MONTH - 1);
        calendar.set(Calendar.DAY_OF_MONTH, DAY);
        calendar.set(Calendar.HOUR_OF_DAY, HOUR);
        calendar.set(Calendar.MINUTE, MINUTE);
        calendar.set(Calendar.SECOND, SECOND);
        calendar.set(Calendar.MILLISECOND, MILLISECOND);
        return calendar;
    }

}