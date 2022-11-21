package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.FirstFlag;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.MCSException;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Class for iterating through DataSets of given provider.
 * <p>
 * The best way to initialise iterator is to obtain it by calling
 * {@link DataSetServiceClient#getDataSetIteratorForProvider(String)} method.
 * <p>
 * Iterator obtains {@link DataSet} objects in chunks, obtaining new chunk only if needed, inside overridden {@link Iterator}
 * class methods.
 */
public class DataSetIterator implements Iterator<DataSet> {

  //iterator parameters
  private final DataSetServiceClient client;
  private final String providerId;
  //variables for holding state
  private final FirstFlag firstTime = new FirstFlag();
  private String nextSlice = null;
  private Iterator<DataSet> dataSetListIterator;

  /**
   * Creates instance of DataSetIterator.
   *
   * @param client properly initialised client for internal communication with MCS server (required)
   * @param providerId id of the provider (required)
   */
  public DataSetIterator(DataSetServiceClient client, String providerId) {
    if (client == null) {
      throw new DriverException("DataSetServiceClient for DataSetIterator cannot be null");
    }
    this.client = client;

    if (providerId == null || providerId.equals("")) {
      throw new DriverException("ProviderId for DataSetIterator cannot be null/empty");
    }
    this.providerId = providerId;

  }

  /**
   * Returns <code>true</code> if the iteration has more elements.
   * <p>
   * The first call to this method might take longer time than the others, as there might be a need to obtain first chunk of
   * data.
   *
   * @return {@code true} if the iteration has more elements, false if not.
   */
  @Override
  public boolean hasNext() {
    if (firstTime.unpack()) {
      obtainNextChunk();
    }

    return (dataSetListIterator.hasNext() || nextSlice != null);
  }

  /**
   * Returns next element in the iteration.
   * <p>
   * Some calls to this method might take longer time than the others, if the new chunk of data has to be obtained.
   *
   * @return next element in the iteration
   * @throws NoSuchElementException if there are no more elements
   */
  @Override
  public DataSet next() {
    if (firstTime.unpack()) {
      obtainNextChunk();
    }
    if (dataSetListIterator.hasNext()) {
      return dataSetListIterator.next();
    }
    if (nextSlice != null) {
      obtainNextChunk();
      return dataSetListIterator.next();
    }
    throw new NoSuchElementException("Calling next on exhausted DataSet iterator.");

  }

  //TODO there is a technical possibility to support this, should we?
  @Override
  public void remove() {
    throw new UnsupportedOperationException("Not supported.");
  }

  //this method does not check if this is valid to obtain next chunk now
  private void obtainNextChunk() {

    ResultSlice<DataSet> currentChunk;
    try {
      currentChunk = client.getDataSetsForProviderChunk(providerId, nextSlice);
    } catch (MCSException ex) {
      throw new DriverException("Error when trying to obtain DataSet list chunk for iterator", ex);
    }
    dataSetListIterator = currentChunk.getResults().iterator();
    nextSlice = currentChunk.getNextSlice();
  }

}
