package eu.europeana.cloud.service.mcs.persistent;

import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

/**
 *
 * @author sielski
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = {"classpath:/spiedServicesTestContext.xml"})
public class CassandraDataSetServiceTest extends CassandraTestBase {

	@Autowired
	private CassandraDataProviderService cassandraDataProviderService;

	@Autowired
	private CassandraRecordService cassandraRecordService;

	@Autowired
	private CassandraDataSetService cassandraDataSetService;

	private static final String providerId = "provider";


	@Before
	public void prepareData() {
		cassandraDataProviderService.createProvider(providerId, new DataProviderProperties());
	}


	@Test
	public void shouldCreateDataSet() {
		// given properties of data set
		String dsName = "ds";
		String description = "description of data set";

		// when new data set is created
		DataSet ds = cassandraDataSetService.createDataSet(providerId, dsName, description);

		// the created data set should properties as given for construction
		assertThat(ds.getId(), is(dsName));
		assertThat(ds.getDescription(), is(description));
		assertThat(ds.getProviderId(), is(providerId));
	}


	@Test(expected = DataSetNotExistsException.class)
	public void shouldNotAssignToNotExistingDataSet() {
		// given all objects exist except for dataset
		Representation r = cassandraRecordService.createRepresentation("cloud-id", "schema", providerId);

		// when trying to add assignment - error is expected
		cassandraDataSetService.
				addAssignment(providerId, "not-existing", r.getRecordId(), r.getSchema(), r.getVersion());
	}


	@Test(expected = RepresentationNotExistsException.class)
	public void shouldNotAssignNotExistingRepresentation() {
		// given all objects exist except for representation
		DataSet ds = cassandraDataSetService.createDataSet(providerId, "ds", "description of this set");

		// when trying to add assignment - error is expected
		cassandraDataSetService.addAssignment(ds.getProviderId(), ds.getId(), "cloud-id", "schema", "version");
	}


	@Test
	public void shouldCreateAndGetDataSet() {
		// given
		String dsName = "ds";
		DataSet ds = cassandraDataSetService.createDataSet(providerId, dsName, "description of this set");

		// when data sets for this provider are fetched
		List<DataSet> dataSets = cassandraDataSetService.getDataSets(providerId, null, 10000).getResults();

		// then those data sets should contain only the one inserted
		assertThat(dataSets.size(), is(1));
		assertThat(dataSets.get(0), is(ds));
	}


	@Test
	public void shouldReturnPagedDataSets() {
		int dataSetCount = 1000;
		List<String> insertedDataSetIds = new ArrayList<>(dataSetCount);

		// insert random data sets
		for (int dsID = 0; dsID < dataSetCount; dsID++) {
			DataSet ds = cassandraDataSetService.createDataSet(providerId, "ds_" + dsID, "description of " + dsID);
			insertedDataSetIds.add(ds.getId());
		}

		// iterate through all data set
		List<String> fetchedDataSets = new ArrayList<>(dataSetCount);
		int sliceSize = 10;
		String token = null;
		do {
			ResultSlice<DataSet> resultSlice = cassandraDataSetService.getDataSets(providerId, token, sliceSize);
			token = resultSlice.getNextSlice();
			assertTrue(resultSlice.getResults().size() == sliceSize || token == null);
			for (DataSet ds : resultSlice.getResults()) {
				fetchedDataSets.add(ds.getId());
			}
		} while (token != null);

		Collections.sort(insertedDataSetIds);
		Collections.sort(fetchedDataSets);
		assertThat(insertedDataSetIds, is(fetchedDataSets));
	}

}
