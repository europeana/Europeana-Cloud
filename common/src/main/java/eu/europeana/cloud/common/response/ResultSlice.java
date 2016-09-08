package eu.europeana.cloud.common.response;

import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.model.Representation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

/**
 * Result slice for methods that would return possibly huge collections of objects that must be divided into paged to
 * handle them.
 *
 * @param <T> Class of returned objects.
 */
@XmlRootElement
@XmlSeeAlso({DataProvider.class, Representation.class, DataSet.class, CloudId.class, LocalId.class, String.class}) // references to all classes that might be used as generics parameters
public class ResultSlice<T> {

	/**
	 * Reference to next slice of result.
	 */
	private String nextSlice;

	/**
	 * List of results in this slice.
	 */
	private List<T> results = new ArrayList<T>();


	/**
	 * Creates a new instance of this class.
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
