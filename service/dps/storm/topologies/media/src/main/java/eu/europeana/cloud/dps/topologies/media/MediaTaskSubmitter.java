package eu.europeana.cloud.dps.topologies.media;

import java.util.Arrays;

import eu.europeana.cloud.client.dps.rest.DpsClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;

public class MediaTaskSubmitter {
	
	public static void main(String[] args) {
		try {
			DpsClient client =
					new DpsClient("http://195.216.97.81:8080/services", "mms_user", "TODO");
			String topologyName = "media_topology";
			
			DpsTask task = new DpsTask();
			task.setTaskName(topologyName + task.getTaskId());
			
			task.addDataEntry(InputDataType.DATASET_URLS,
					Arrays.asList("https://test-cloud.europeana.eu/api/data-providers/mms_prov/data-sets/mms_set"));
			
			long taskId = client.submitTask(task, topologyName);
			
			System.out.println("OK! " + taskId);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
