package eu.europeana.cloud.common.utils;


import java.time.format.DateTimeParseException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

class DateAdapterTest {

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
        assertEquals(DATE_STRING, dateAdapter.marshal(DATE));
    }

    @Test
    void shouldDeSerializeNegativeNumberOfMillisendsSuccessfully() {
        Date date = dateAdapter.unmarshal("-1695479578123");

        assertEquals(new Date(-1695479578123L), date);
    }

    @Test
    void shouldDeserializeISODate(){
        Date date=dateAdapter.unmarshal("2021-02-16T12:58:58.321Z");

        assertEquals(DATE, date);
    }

    @Test
    void shouldThrowParsingException() {
        String unParsedDateString = "2017-11-23";
        assertThrows(DateTimeParseException.class, () -> dateAdapter.unmarshal(unParsedDateString));
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