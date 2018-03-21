package eu.europeana.cloud.dps.topologies.media.support;

import java.util.Arrays;

import eu.europeana.cloud.client.dps.rest.DpsClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;

public class MediaTaskSubmitter {
	
	public static void main(String[] args) {
		try {
			DpsClient client =
					new DpsClient("http://195.216.97.81:8080/services", "mms_user", "");
			String topologyName = "media_topology";
			
			DpsTask task = new DpsTask();
			task.setTaskName(topologyName + task.getTaskId());
			
			task.addDataEntry(InputDataType.DATASET_URLS,
					Arrays.asList("https://test-cloud.europeana.eu/api/data-providers/mms_prov/data-sets/mms_set"));
			
//			HashMap<String, String> params = new HashMap<>();
//			params.put("host.limit.dlibrary.ascsa.edu.gr", "3");
//			task.setParameters(params);
			
			long taskId = client.submitTask(task, topologyName);
			
			System.out.println("OK! " + taskId);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
