package eu.europeana.cloud.common.utils;


import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

public class DateAdapterTest {

    //example string representing date that is serialized to xml/json. It is number of ms from 1970
    public static final String DATE_STRING = "1613480338321";

    //DATE reprezented by field DATE_STRING
    public static final Date DATE = new Date(1613480338321L);

    private static DateAdapter dateAdapter;

    @BeforeAll
    static void init() {
        dateAdapter = new DateAdapter();
    }

    @Test
    void shouldSerializeTheDateSuccessfully() {
        Date date = dateAdapter.unmarshal(DATE_STRING);
        assertEquals(DATE, date);
    }

    @Test
    void shouldDeSerializeTheDateSuccessfully() {
        assertEquals(dateAdapter.marshal(DATE), DATE_STRING);
    }

    @Test
    void shouldThrowParsingException() {
        String unParsedDateString = "2017-11-23";
        assertThrows(NumberFormatException.class, () -> dateAdapter.unmarshal(unParsedDateString));
    }

    @Test
    void shouldCreateNullDateInCaseEmptyOrNull() {
        assertNull(dateAdapter.unmarshal(null));
        assertNull(dateAdapter.unmarshal(""));
    }

    @Test
    void shouldThrowRunTimeException() {
        assertThrows(RuntimeException.class, () -> dateAdapter.marshal(null));
    }

}