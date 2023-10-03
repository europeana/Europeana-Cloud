package eu.europeana.cloud.service.mcs.controller;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import java.math.BigInteger;
import org.junit.Test;

/**
 * ContentRangeTest
 */
public class ContentRangeTest {

  @Test
  public void testParsingProperRange()
      throws WrongContentRangeException {
    FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=1-2");
    assertThat(range.getStart(), is(1L));
    assertThat(range.getEnd(), is(2L));
  }


  @Test
  public void testParsingLongNumbers()
      throws WrongContentRangeException {
    Long start = Integer.MAX_VALUE * 2L;
    Long end = Integer.MAX_VALUE * 3L;
    FileResource.ContentRange range = FileResource.ContentRange.parse(String.format("bytes=%s-%s", start, end));
    assertThat(range.getStart(), is(start));
    assertThat(range.getEnd(), is(end));
  }


  @Test
  public void testParsingOffset()
      throws WrongContentRangeException {
    FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=1234-");
    assertThat(range.getStart(), is(1234L));
    assertThat(range.getEnd(), is(-1L));
  }


  @Test
  public void testParsingSingleByte()
      throws WrongContentRangeException {
    FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=1234-1234");
    assertThat(range.getStart(), is(1234L));
    assertThat(range.getEnd(), is(1234L));
  }


  @Test(expected = WrongContentRangeException.class)
  public void testErrorOnParsingWhenNumberTooLarge()
      throws WrongContentRangeException {
    BigInteger tooLargeNumber = new BigInteger("" + Long.MAX_VALUE).shiftLeft(1);
    FileResource.ContentRange range = FileResource.ContentRange.parse(tooLargeNumber.toString() + "-");
  }


  @Test(expected = WrongContentRangeException.class)
  public void testErrorOnParsingWrongFormat()
      throws WrongContentRangeException {
    FileResource.ContentRange range = FileResource.ContentRange.parse("1234-");
  }


  @Test(expected = WrongContentRangeException.class)
  public void testErrorOnParsingWrongFormat1()
      throws WrongContentRangeException {
    FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=-1234");
  }


  @Test(expected = WrongContentRangeException.class)
  public void testErrorOnParsingWrongFormat2()
      throws WrongContentRangeException {
    FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=-1234-2000");
  }


  @Test(expected = WrongContentRangeException.class)
  public void testErrorOnParsingWrongFormat3()
      throws WrongContentRangeException {
    FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=1234--2000");
  }


  @Test(expected = WrongContentRangeException.class)
  public void testErrorOnParsingWrongRange()
      throws WrongContentRangeException {
    FileResource.ContentRange range = FileResource.ContentRange.parse("bytes=1234-2");
  }
}
