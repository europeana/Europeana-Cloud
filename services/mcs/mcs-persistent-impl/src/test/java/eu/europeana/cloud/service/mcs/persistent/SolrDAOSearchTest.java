package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.solr.client.solrj.SolrServerException;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/solrTestContext.xml"})
public class SolrDAOSearchTest {

	@Autowired
	private SolrDAO solrDAO;


	@Before
	public void insertSampleData()
			throws IOException, SolrServerException {

	}


	@Test
	public void searchBySchema()
			throws IOException, SolrServerException {
		Representation r1 = insertRepresentation("c1", "dc", "v1", "dp", true, new Date());
		Representation r2 = insertRepresentation("c1", "dc", "v2", "dp", true, new Date());
		Representation r3 = insertRepresentation("c1", "jpg", "v3", "dp", true, new Date());
		Representation r4 = insertRepresentation("c1", "jpg", "v4", "dp", true, new Date());

		List<Representation> foundRepresentations
				= solrDAO.search(RepresentationSearchParams.builder().setSchema("dc").build(), 0, 10);
		assertSameSet(Arrays.asList(r1, r2), foundRepresentations);
	}


	@Test
	public void searchByProvider()
			throws IOException, SolrServerException {
		Representation r1 = insertRepresentation("c1", "dc", "v1", "dp1", true, new Date());
		Representation r2 = insertRepresentation("c1", "dc", "v2", "dp", true, new Date());
		Representation r3 = insertRepresentation("c1", "jpg", "v3", "dp", true, new Date());
		Representation r4 = insertRepresentation("c1", "jpg", "v4", "dp1", true, new Date());

		List<Representation> foundRepresentations = solrDAO.search(RepresentationSearchParams.builder().
				setDataProvider("dp1").build(), 0, 10);
		assertSameSet(Arrays.asList(r1, r4), foundRepresentations);
	}


	@Test
	public void searchBySchemaAndProvider()
			throws IOException, SolrServerException {
		Representation r1 = insertRepresentation("c1", "dc", "v1", "dp1", true, new Date());
		Representation r2 = insertRepresentation("c1", "dc", "v2", "dp", true, new Date());
		Representation r3 = insertRepresentation("c1", "jpg", "v3", "dp", true, new Date());
		Representation r4 = insertRepresentation("c1", "jpg", "v4", "dp1", true, new Date());

		List<Representation> foundRepresentations = solrDAO.
				search(RepresentationSearchParams.builder().setDataProvider("dp1").setSchema("dc").build(), 0, 10);
		assertSameSet(Arrays.asList(r1), foundRepresentations);
	}


	@Test
	public void searchByDate()
			throws IOException, SolrServerException {
		Calendar c = GregorianCalendar.getInstance();
		c.set(2000, 05, 15, 15, 15);
		Representation r1 = insertRepresentation("c1", "dc", "v1", "dp1", true, c.getTime());
		c.set(2000, 05, 15, 15, 17);
		Representation r2 = insertRepresentation("c1", "dc", "v2", "dp1", true, c.getTime());
		c.set(2001, 05, 15, 15, 0);
		Representation r3 = insertRepresentation("c1", "dc", "v3", "dp1", true, c.getTime());
		c.set(2002, 05, 15, 15, 15);
		Representation r4 = insertRepresentation("c1", "dc", "v4", "dp1", true, c.getTime());
		List<Representation> foundRepresentations = solrDAO.
				search(RepresentationSearchParams.builder()
						.setFromDate(r2.getCreationDate())
						.setToDate(r3.getCreationDate())
						.build(), 0, 10);
		assertSameSet(Arrays.asList(r2, r3), foundRepresentations);
	}


	private <T> void assertSameSet(Collection<? extends T> c1, Collection<? extends T> c2) {
		Set<T> c1Set = new HashSet<>(c1);
		Set<T> c2Set = new HashSet<>(c2);
		assertThat(c1Set, is(c2Set));
	}


	private Representation insertRepresentation(String cloudId, String schema, String version, String dataProvider, boolean persistent, Date date)
			throws IOException, SolrServerException {
		Representation rep = new Representation(cloudId, schema, version, null, null, dataProvider, new ArrayList<File>(), persistent, date);
		solrDAO.insertRepresentation(rep, null);
		return rep;
	}

}
