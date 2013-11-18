package eu.europeana.cloud.common.response;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

/**
 *
 * @author sielski
 */
@XmlRootElement
@XmlSeeAlso({DataProvider.class, Representation.class, DataSet.class})
public class ResultSlice<T> {

	private String nextSlice;

	private List<T> results = new ArrayList<T>();


	public ResultSlice(String nextSlice, List<T> results) {
		this.nextSlice = nextSlice;
		this.results = Collections.unmodifiableList(results);
	}


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
