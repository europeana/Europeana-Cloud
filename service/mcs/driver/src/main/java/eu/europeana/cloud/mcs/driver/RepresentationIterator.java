package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.FirstFlag;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Class for iterating through Representations of given data set.
 * <p>
 * The best way to initialise iterator is to obtain it by calling
 * {@link DataSetServiceClient#getRepresentationIterator(String providerId, String dataSetId)} method.
 * <p>
 * Iterator obtains {@link Representation} objects in chunks, obtaining new chunk only if needed, inside overridden
 * {@link Iterator} class methods.
 */
public class RepresentationIterator implements Iterator<Representation> {

  //iterator parameters
  private final DataSetServiceClient client;
  private final String providerId;
  private final String dataSetId;
  //variables for holding state
  private final FirstFlag firstTime = new FirstFlag();
  private String nextSlice = null;
  private Iterator<Representation> representationListIterator;

  /**
   * Creates instance of RepresentationIterator.
   *
   * @param client properly initialised client for internal communication with MCS server (required)
   * @param providerId id of the provider (required)
   * @param dataSetId data set identifier (required)
   */
  public RepresentationIterator(DataSetServiceClient client, String providerId, String dataSetId) {
    if (client == null) {
      throw new DriverException("DataSetServiceClient for RepresentationIterator cannot be null");
    }
    this.client = client;

    if (providerId == null || providerId.equals("")) {
      throw new DriverException("ProviderId for RepresentationIterator cannot be null/empty");
    }
    this.providerId = providerId;

    if (dataSetId == null || dataSetId.equals("")) {
      throw new DriverException("ProviderId for RepresentationIterator cannot be null/empty");
    }
    this.dataSetId = dataSetId;

  }

  /**
   * Returns <code>true</code> if the iteration has more elements.
   * <p>
   * Some calls to this method might take longer time than the others, if the new chunk of data has to be obtained. If data set
   * does not exists, first call to this method will throw {@link DriverException} with inner exception:
   * {@link DataSetNotExistsException}.
   *
   * @return {@code true} if the iteration has more elements, false if not.
   */
  @Override
  public boolean hasNext() {
    if (firstTime.unpack()) {
      obtainNextChunk();
    }
    if (representationListIterator.hasNext()) {
      return true;
    }
    if (nextSlice != null) {
      obtainNextChunk();
      return representationListIterator.hasNext();
    }
    return false;
  }

  /**
   * Returns next element in the iteration.
   * <p>
   * Some calls to this method might take longer time than the others, if the new chunk of data has to be obtained. If data set
   * does not exists, first call to this method will throw {@link DriverException} with inner exception:
   * {@link DataSetNotExistsException}.
   *
   * @return next element in the iteration
   * @throws NoSuchElementException if there are no more elements
   */
  @Override
  public Representation next() {
    if (firstTime.unpack()) {
      obtainNextChunk();
    }
    if (representationListIterator.hasNext()) {
      return representationListIterator.next();
    }
    if (nextSlice != null) {
      obtainNextChunk();
      return representationListIterator.next();
    }
    throw new NoSuchElementException("Calling next on exhausted Representation iterator.");

  }

  //TODO there is a technical possibility to support this, should we?
  @Override
  public void remove() {
    throw new UnsupportedOperationException("Not supported.");
  }

  //this method does not check if this is valid to obtain next chunk now
  private void obtainNextChunk() {

    ResultSlice<Representation> currentChunk;
    try {
      currentChunk = client.getDataSetRepresentationsChunk(providerId, dataSetId, nextSlice);
    } catch (DataSetNotExistsException ex) {
      throw new DriverException("Data set does not exist.", ex);
    } catch (MCSException ex) {
      throw new DriverException("Error when trying to obtain Representation list chunk for iterator", ex);
    }

    representationListIterator = currentChunk.getResults().iterator();
    nextSlice = currentChunk.getNextSlice();
  }

}
