package eu.europeana.cloud.common.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.model.Representation;
import jakarta.xml.bind.annotation.XmlRootElement;
import jakarta.xml.bind.annotation.XmlSeeAlso;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Result slice for methods that would return possibly huge collections of objects that must be divided into paged to handle
 * them.
 *
 * @param <T> Class of returned objects.
 */
@XmlRootElement
@XmlSeeAlso({DataProvider.class, Representation.class, DataSet.class, CloudId.class,
    LocalId.class, String.class, CloudVersionRevisionResponse.class, CloudTagsResponse.class})
// references to all classes that might be used as generics parameters
@JsonRootName("resultSlice")
@JacksonXmlRootElement
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ResultSlice<T> {

  /**
   * Reference to next slice of result.
   */
  private String nextSlice;

  /**
   * List of results in this slice.
   */
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<T> results = new ArrayList<>();


  /**
   * Creates a new instance of this class.
   *
   * @param nextSlice
   * @param results
   */
  public ResultSlice(String nextSlice, List<T> results) {
    this.nextSlice = nextSlice;
    this.results = Collections.unmodifiableList(results);
  }


  /**
   * Creates a new instance of this class.
   */
  public ResultSlice() {
  }


  public String getNextSlice() {
    return nextSlice;
  }


  public List<T> getResults() {
    return results;
  }


  public void setNextSlice(String nextSlice) {
    this.nextSlice = nextSlice;
  }


  public void setResults(List<T> results) {
    this.results = results;
  }

}
