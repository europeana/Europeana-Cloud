package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.Row;
import com.google.common.collect.Streams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class BucketRecordIteratorTest {

  private Row ROW_A = row();
  private Row ROW_B = row();
  private Row ROW_C = row();
  private Row ROW_D = row();
  private Row ROW_E = row();
  private Row ROW_F = row();

  @Mock
  private IntFunction<Iterator<Row>> queryMethod;

  @Mock
  private BucketRecordIterator.RowConverter<Row> convertMethod;

  private BucketRecordIterator<Row> iterator;

  @BeforeEach
  void setup() {
    iterator = new BucketRecordIterator<>(4, queryMethod, convertMethod);
    when(queryMethod.apply(anyInt())).thenReturn(Collections.emptyIterator());
    when(convertMethod.apply(any(Row.class))).thenAnswer(invocation -> invocation.getArgument(0));
  }

  @Test
  void hasNextShouldReturnFalseWhenEveryBucketIsEmpty() {
    assertFalse(iterator.hasNext());
  }

  @Test
  void nextShouldThrowNoSuchElementExceptionWhenEveryBucketIsEmpty() {
    assertThrows(NoSuchElementException.class, () -> iterator.next());
  }

  @Test
  void shouldQueryAllBucketsWhenTestingHasNextEvenIfEveryBucketIsEmpty() {
    iterator.hasNext();

    verify(queryMethod, times(4)).apply(anyInt());
  }

  @Test
  void shouldQueryEveryBucketWhenExecuteNextEvenIfEveryBucketIsEmpty() {
    try {
      iterator.next();
    } catch (NoSuchElementException e) {
      //this is expected situation
    }

    verify(queryMethod).apply(0);
    verify(queryMethod).apply(1);
    verify(queryMethod).apply(2);
    verify(queryMethod).apply(3);
  }

  @Test
  void shouldReturnElementOfFirstBucket() {
    when(queryMethod.apply(0)).thenReturn(Arrays.asList(ROW_A).iterator());

    assertTrue(iterator.hasNext());
    assertEquals(ROW_A, iterator.next());
    assertFalse(iterator.hasNext());
  }

  @Test
  void shouldReturnElementOfMiddleBucket() {
    when(queryMethod.apply(1)).thenReturn(Arrays.asList(ROW_A).iterator());

    assertTrue(iterator.hasNext());
    assertEquals(ROW_A, iterator.next());
    assertFalse(iterator.hasNext());
  }


  @Test
  void shouldReturnElementOfLastBucket() {
    when(queryMethod.apply(3)).thenReturn(Arrays.asList(ROW_A).iterator());

    assertTrue(iterator.hasNext());
    assertEquals(ROW_A, iterator.next());
    assertFalse(iterator.hasNext());
  }


  @Test
  void shouldReturnAllElementsOfFirstBucket() {
    when(queryMethod.apply(0)).thenReturn(Arrays.asList(ROW_A, ROW_B).iterator());

    assertTrue(iterator.hasNext());
    assertEquals(ROW_A, iterator.next());
    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasNext());
    assertEquals(ROW_B, iterator.next());
    assertFalse(iterator.hasNext());
  }

  @Test
  void shouldReturnAllElementsOfMiddleBucket() {
    when(queryMethod.apply(2)).thenReturn(Arrays.asList(ROW_A, ROW_B).iterator());

    assertTrue(iterator.hasNext());
    assertEquals(ROW_A, iterator.next());
    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasNext());
    assertEquals(ROW_B, iterator.next());
    assertFalse(iterator.hasNext());
  }

  @Test
  void shouldReturnAllElementsOfLastBucket() {
    when(queryMethod.apply(3)).thenReturn(Arrays.asList(ROW_A, ROW_B).iterator());

    assertTrue(iterator.hasNext());
    assertEquals(ROW_A, iterator.next());
    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasNext());
    assertEquals(ROW_B, iterator.next());
    assertFalse(iterator.hasNext());
  }


  @Test
  void shouldReturnAllElementsOfEveryBucket() {
    when(queryMethod.apply(0)).thenReturn(Arrays.asList(ROW_A, ROW_B).iterator());
    when(queryMethod.apply(1)).thenReturn(Arrays.asList(ROW_C).iterator());
    when(queryMethod.apply(2)).thenReturn(Arrays.asList(ROW_D).iterator());
    when(queryMethod.apply(3)).thenReturn(Arrays.asList(ROW_E, ROW_F).iterator());

    assertTrue(iterator.hasNext());
    assertEquals(ROW_A, iterator.next());
    assertTrue(iterator.hasNext());
    assertEquals(ROW_B, iterator.next());
    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasNext());
    assertEquals(ROW_C, iterator.next());
    assertTrue(iterator.hasNext());
    assertTrue(iterator.hasNext());
    assertEquals(ROW_D, iterator.next());
    assertTrue(iterator.hasNext());
    assertEquals(ROW_E, iterator.next());
    assertTrue(iterator.hasNext());
    assertEquals(ROW_F, iterator.next());
    assertFalse(iterator.hasNext());
    assertFalse(iterator.hasNext());
  }

  @Test
  void shouldQueryValidBuckets() {
    when(queryMethod.apply(0)).thenReturn(Arrays.asList(ROW_A, ROW_B).iterator());
    when(queryMethod.apply(1)).thenReturn(Arrays.asList(ROW_C).iterator());
    when(queryMethod.apply(2)).thenReturn(Arrays.asList(ROW_D).iterator());
    when(queryMethod.apply(3)).thenReturn(Arrays.asList(ROW_E, ROW_F).iterator());

    //consume iterator
    Streams.stream(iterator).count();

    verify(queryMethod, times(4)).apply(anyInt());
    verify(queryMethod).apply(0);
    verify(queryMethod).apply(1);
    verify(queryMethod).apply(2);
    verify(queryMethod).apply(3);
  }

  private static Row row() {
    return Mockito.mock(Row.class);
  }

}