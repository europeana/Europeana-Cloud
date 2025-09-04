package eu.europeana.cloud.mcs.driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.notNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class RepresentationIteratorTest {

  public static final String NEXT_SLICE = "nextSlice";
  private DataSetServiceClient dataSetServiceClient;
  private static final String PROVIDER = "PROVIDER";
  private static final String DATASET = "DATASET";


  @Before
  public void init() {
    dataSetServiceClient = mock(DataSetServiceClient.class);
  }

  @Test
  public void testRepresentationIteratorForTheFirstIteration() throws Exception {
    RepresentationIterator representationIterator = new RepresentationIterator(dataSetServiceClient, PROVIDER, DATASET);
    ResultSlice<Representation> representationResultSlice = getFinalRepresentationResultSlice(2);

    when(dataSetServiceClient.getDataSetRepresentationsChunk(PROVIDER, DATASET, false, null)).thenReturn(representationResultSlice);
    int count = 0;

    while (representationIterator.hasNext()) {
      assertTrue(representationIterator.hasNext());
      representationIterator.next();
      count++;
    }
    assertEquals(count, 2);
    verify(dataSetServiceClient, times(0)).getDataSetRepresentationsChunk(eq(PROVIDER), eq(DATASET), false, notNull(String.class));
    assertFalse(representationIterator.hasNext());


  }

  @Test
  public void testRepresentationIteratorWhenTheSecondIterationIsEmpty() throws Exception {
    RepresentationIterator representationIterator = new RepresentationIterator(dataSetServiceClient, PROVIDER, DATASET);

    ResultSlice<Representation> representationResultSlice = getRepresentationResultSlice(100);

    when(dataSetServiceClient.getDataSetRepresentationsChunk(PROVIDER, DATASET, false, null)).thenReturn(representationResultSlice);

    ResultSlice<Representation> emptyResultSet = new ResultSlice<>();
    emptyResultSet.setResults(new ArrayList<Representation>());
    when(dataSetServiceClient.getDataSetRepresentationsChunk(PROVIDER, DATASET, false, NEXT_SLICE)).thenReturn(
            emptyResultSet);

    int count = 0;
    while (representationIterator.hasNext()) {
      assertTrue(representationIterator.hasNext());
      representationIterator.next();
      count++;
    }
    assertEquals(100, count);
    verify(dataSetServiceClient, times(1)).getDataSetRepresentationsChunk(PROVIDER, DATASET, false, NEXT_SLICE);
    assertFalse(representationIterator.hasNext());


  }


  @Test
  public void testRepresentationIteratorWhenTheSecondIterationNotEmpty() throws Exception {
    RepresentationIterator representationIterator = new RepresentationIterator(dataSetServiceClient, PROVIDER, DATASET);

    ResultSlice<Representation> representationResultSlice = getRepresentationResultSlice(100);
    when(dataSetServiceClient.getDataSetRepresentationsChunk(PROVIDER, DATASET, false, null)).thenReturn(representationResultSlice);

    ResultSlice<Representation> nextResultSet = getFinalRepresentationResultSlice(5);
    when(dataSetServiceClient.getDataSetRepresentationsChunk(PROVIDER, DATASET, false, NEXT_SLICE)).thenReturn(
            nextResultSet);

    int count = 0;

    while (representationIterator.hasNext()) {
      assertTrue(representationIterator.hasNext());
      representationIterator.next();
      count++;
    }
    assertEquals(105, count);
    verify(dataSetServiceClient, times(1)).getDataSetRepresentationsChunk(PROVIDER, DATASET, false, NEXT_SLICE);
    assertFalse(representationIterator.hasNext());


  }


  private ResultSlice<Representation> getRepresentationResultSlice(int ItemNumber) {
    List<Representation> representationList = new ArrayList<>(ItemNumber);
    for (int i = 0; i < ItemNumber; i++) {
      representationList.add(new Representation());
    }

    ResultSlice<Representation> representationResultSlice = new ResultSlice<>();
    representationResultSlice.setResults(representationList);
    representationResultSlice.setNextSlice(NEXT_SLICE);
    return representationResultSlice;
  }

  private ResultSlice<Representation> getFinalRepresentationResultSlice(int ItemNumber) {
    ResultSlice<Representation> representationResultSlice = getRepresentationResultSlice(ItemNumber);
    representationResultSlice.setNextSlice(null);
    return representationResultSlice;
  }


}