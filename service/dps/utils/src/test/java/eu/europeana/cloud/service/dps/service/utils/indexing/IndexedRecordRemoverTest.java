package eu.europeana.cloud.service.dps.service.utils.indexing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexingException;
import eu.europeana.metis.utils.DepublicationReason;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class IndexedRecordRemoverTest {

  public static final long TASK_ID = 10L;
  private static final String RECORD_ID = "/100/record1";
  private static final DepublicationReason REASON = DepublicationReason.PERMISSION_ISSUES;
  private static final TargetIndexingDatabase TARGET_DB = TargetIndexingDatabase.PREVIEW;

  @Mock
  private IndexWrapper indexWrapper;

  @Mock
  private Indexer indexer;

  @Mock
  private FullBeanImpl tombstone;

  @InjectMocks
  private IndexedRecordRemover indexedRecordRemover;

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
    when(indexWrapper.getIndexer(Mockito.any())).thenReturn(indexer);
  }

  @Test
  public void shouldCreateTombstoneAndRemoveRecordForPreviewDB() throws Exception {
    when(indexer.indexTombstone(any(), any())).thenReturn(true);
    when(indexer.remove(any())).thenReturn(true);

    boolean result = indexedRecordRemover.removeRecord(TargetIndexingDatabase.PREVIEW, RECORD_ID, REASON);

    //then
    assertTrue(result);
    verify(indexWrapper).getIndexer(TargetIndexingDatabase.PREVIEW);
    verify(indexer).indexTombstone(RECORD_ID, REASON);
    verify(indexer).remove(RECORD_ID);
  }

  @Test
  public void shouldCreateTombstoneAndRemoveRecordForPublishDB() throws Exception {
    when(indexer.indexTombstone(any(), any())).thenReturn(true);
    when(indexer.remove(any())).thenReturn(true);

    boolean result = indexedRecordRemover.removeRecord(TargetIndexingDatabase.PUBLISH, RECORD_ID, REASON);

    //then
    assertTrue(result);
    verify(indexWrapper).getIndexer(TargetIndexingDatabase.PUBLISH);
    verify(indexer).indexTombstone(RECORD_ID, REASON);
    verify(indexer).remove(RECORD_ID);
  }

  @Test
  public void shouldRemoveRecordWhenCouldNotCreateNewTombstoneButTombstoneAlreadyExists() throws Exception {
    when(indexer.indexTombstone(any(), any())).thenReturn(false);
    when(indexer.getTombstone(any())).thenReturn(tombstone);
    when(indexer.remove(any())).thenReturn(false);

    //when
    boolean result = indexedRecordRemover.removeRecord(TARGET_DB, RECORD_ID, REASON);

    //then
    assertTrue(result);
    verify(indexWrapper).getIndexer(TARGET_DB);
    verify(indexer).indexTombstone(RECORD_ID, REASON);
    verify(indexer).getTombstone(RECORD_ID);
    verify(indexer).remove(RECORD_ID);
  }


  @Test
  public void shouldNotRemoveRecordWhenCouldNotCreateNewTombstoneAndTombstoneNotExists() throws Exception {
    when(indexer.indexTombstone(any(), any())).thenReturn(false);
    when(indexer.getTombstone(any())).thenReturn(null);

    //when
    boolean result = indexedRecordRemover.removeRecord(TARGET_DB, RECORD_ID, REASON);

    //then
    assertFalse(result);
    verify(indexWrapper).getIndexer(TARGET_DB);
    verify(indexer).indexTombstone(RECORD_ID, REASON);
    verify(indexer).getTombstone(RECORD_ID);
    verify(indexer, never()).remove(RECORD_ID);
  }


  @Test
  public void shouldNotRemoveRecordWhenIndexTombstoneThrowsException() throws Exception {
    when(indexer.indexTombstone(any(), any())).thenThrow(new IndexingException("") {
    });

    assertThrows(IndexingException.class, () -> indexedRecordRemover.removeRecord(TARGET_DB, RECORD_ID, REASON));

    verify(indexer, never()).remove(RECORD_ID);
  }

}
