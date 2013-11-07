package eu.europeana.cloud.service.mcs.rest;

import java.math.BigInteger;
import org.junit.Test;
//import static junitparams.JUnitParamsRunner.$;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
//import static org.mockito.Mockito.times;

/**
 * ContentRangeTest
 */
public class ContentRangeTest {

    @Test
    public void testParsingProperRange() {
        FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=1-2");
        assertThat(range.start, is(1L));
        assertThat(range.end, is(2L));
    }


    @Test
    public void testParsingLongNumbers() {
        Long start = Integer.MAX_VALUE * 2L;
        Long end = Integer.MAX_VALUE * 3L;
        FileResource.ContentRange range = FileResource.ContentRange.parse(String.format("bytes=%s-%s", start, end));
        assertThat(range.start, is(start));
        assertThat(range.end, is(end));
    }


    @Test
    public void testParsingOffset() {
        FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=1234-");
        assertThat(range.start, is(1234L));
        assertThat(range.end, is(-1L));
    }


    @Test
    public void testParsingSingleByte() {
        FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=1234-1234");
        assertThat(range.start, is(1234L));
        assertThat(range.end, is(1234L));
    }


    @Test(expected = WrongContentRangeException.class)
    public void testErrorOnParsingWhenNumberTooLarge() {
        BigInteger tooLargeNumber = new BigInteger("" + Long.MAX_VALUE).shiftLeft(1);
        FileResource.ContentRange range = FileResource.ContentRange.parse(tooLargeNumber.toString() + "-");
    }


    @Test(expected = WrongContentRangeException.class)
    public void testErrorOnParsingWrongFormat() {
        FileResource.ContentRange range = FileResource.ContentRange.parse("1234-");
    }


    @Test(expected = WrongContentRangeException.class)
    public void testErrorOnParsingWrongFormat1() {
        FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=-1234");
    }


    @Test(expected = WrongContentRangeException.class)
    public void testErrorOnParsingWrongFormat2() {
        FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=-1234-2000");
    }


    @Test(expected = WrongContentRangeException.class)
    public void testErrorOnParsingWrongFormat3() {
        FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=1234--2000");
    }


    @Test(expected = WrongContentRangeException.class)
    public void testErrorOnParsingWrongRange() {
        FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=1234-2");
    }
}
