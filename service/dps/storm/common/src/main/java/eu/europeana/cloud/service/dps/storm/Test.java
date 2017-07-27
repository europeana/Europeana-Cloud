package eu.europeana.cloud.service.dps.storm;

import com.google.gson.Gson;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;

/**
 * Created by Tarek on 7/26/2017.
 */
public class Test {
    public static void main(String[] args) throws Exception {
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
        oaipmhHarvestingDetails.setSchemas(new HashSet<>(Arrays.asList("Schema")));
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Date start = sdf.parse("15/1/2012");
        System.out.println(start.getTime());
        Date end = sdf.parse("1/4/2012");
        System.out.println(end.getTime());
        oaipmhHarvestingDetails.setDateFrom(start);
        oaipmhHarvestingDetails.setDateUntil(end);
        Gson gson = new Gson();
        System.out.println(gson.toJson(oaipmhHarvestingDetails));
    }
}
