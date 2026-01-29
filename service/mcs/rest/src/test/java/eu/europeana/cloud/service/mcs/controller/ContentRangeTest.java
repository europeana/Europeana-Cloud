package eu.europeana.cloud.service.mcs.controller;

import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * ContentRangeTest
 */
class ContentRangeTest {

    @Test
    void testParsingProperRange()
            throws WrongContentRangeException {
        FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=1-2");
        assertThat(range.getStart(), is(1L));
        assertThat(range.getEnd(), is(2L));
    }


    @Test
    void testParsingLongNumbers()
            throws WrongContentRangeException {
        Long start = Integer.MAX_VALUE * 2L;
        Long end = Integer.MAX_VALUE * 3L;
        FileResource.ContentRange range = FileResource.ContentRange.parse(String.format("bytes=%s-%s", start, end));
        assertThat(range.getStart(), is(start));
        assertThat(range.getEnd(), is(end));
    }


    @Test
    void testParsingOffset()
            throws WrongContentRangeException {
        FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=1234-");
        assertThat(range.getStart(), is(1234L));
        assertThat(range.getEnd(), is(-1L));
    }


    @Test
    void testParsingSingleByte()
            throws WrongContentRangeException {
        FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=1234-1234");
        assertThat(range.getStart(), is(1234L));
        assertThat(range.getEnd(), is(1234L));
    }


    @Test
    void testErrorOnParsingWhenNumberTooLarge() {
        BigInteger tooLargeNumber = new BigInteger("" + Long.MAX_VALUE).shiftLeft(1);

        assertThrows(
                WrongContentRangeException.class,
                () -> FileResource.ContentRange.parse(tooLargeNumber.toString() + "-")
        );
    }

    @Test
    void testErrorOnParsingWrongFormat() {
        assertThrows(
                WrongContentRangeException.class,
                () -> FileResource.ContentRange.parse("1234-")
        );
    }

    @Test
    void testErrorOnParsingWrongFormat1() {
        assertThrows(
                WrongContentRangeException.class,
                () -> FileResource.ContentRange.parse("bytes=-1234")
        );
    }

    @Test
    void testErrorOnParsingWrongFormat2() {
        assertThrows(
                WrongContentRangeException.class,
                () -> FileResource.ContentRange.parse("bytes=-1234-2000")
        );
    }

    @Test
    void testErrorOnParsingWrongFormat3() {
        assertThrows(
                WrongContentRangeException.class,
                () -> FileResource.ContentRange.parse("bytes=1234--2000")
        );
    }

    @Test
    void testErrorOnParsingWrongRange() {
        assertThrows(
                WrongContentRangeException.class,
                () -> FileResource.ContentRange.parse("bytes=1234-2")
        );
    }
}
