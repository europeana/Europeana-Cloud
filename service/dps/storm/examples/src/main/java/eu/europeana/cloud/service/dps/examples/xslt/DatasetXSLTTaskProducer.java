package eu.europeana.cloud.service.dps.examples.xslt;

import eu.europeana.cloud.client.dps.rest.DpsClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.exception.DpsException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DatasetXSLTTaskProducer {

	public static void main(String[] args) throws DpsException {

		// args[0]: dpsUrl (e.g., http://146.48.82.158:8080/ecloud-service-dps-rest-0.3-SNAPSHOT)
		// args[1]: topology name (e.g., franco_maria_topic)
		
		// args[2]: file containing record URLs (one per line)
		// args[3]: XSLT URL (all records will be processed by this XSLT)

		// args[4]: username (e.g admin)
		// args[5]: password (e.g admin)
		
		String dpsUrl = args[0];
		String topologyName = args[1];
		String username = args[4];
		String password = args[5];

		DpsTask task = new DpsTask();
		task.setTaskName("xslt_transformation-" + task.getTaskId());

		String line = "";
		BufferedReader br;
		List<String> records = new ArrayList<String>();

		try {
			br = new BufferedReader(new FileReader(args[2]));
			while ((line = br.readLine()) != null) {
				records.add(line.trim());
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		task.addDataEntry(InputDataType.FILE_URLS, records);
		task.addParameter("XSLT_URL", args[3]);

		DpsClient dps = new DpsClient(dpsUrl, username, password);
		dps.submitTask(task, topologyName);
	}
}
