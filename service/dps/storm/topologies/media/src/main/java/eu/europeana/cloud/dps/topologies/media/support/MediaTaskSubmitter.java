package eu.europeana.cloud.dps.topologies.media.support;

import java.io.IOException;
import java.io.InputStream;

import org.codehaus.jackson.map.ObjectMapper;

import eu.europeana.cloud.client.dps.rest.DpsClient;
import eu.europeana.cloud.service.dps.DpsTask;

public class MediaTaskSubmitter {

    public static void main(String[] args) {
        try {
            DpsClient client =
                    new DpsClient("http://195.216.97.81:8080/services", "mms_user", "");
            String topologyName = "media_topology";

            DpsTask task = new DpsTask();
            String configFileName = "dummy-task.json";
            try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFileName)) {
                task = new ObjectMapper().readValue(is, DpsTask.class);
            } catch (IOException e) {
                throw new RuntimeException("Built in config could not be loaded: " + configFileName, e);
            }

            task.setTaskName(topologyName + task.getTaskId());

            long taskId = client.submitTask(task, topologyName);

            System.out.println("OK! " + taskId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
