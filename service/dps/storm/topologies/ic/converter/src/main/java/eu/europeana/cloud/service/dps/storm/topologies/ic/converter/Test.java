package eu.europeana.cloud.service.dps.storm.topologies.ic.converter;

import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter.ConverterContext;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter.KakaduConverterTiffToJP2;

/**
 * Created by Tarek on 4/6/2016.
 */
public class Test {
    public static void main(String[] args) throws Exception
    {
        ConverterContext context =new ConverterContext(new KakaduConverterTiffToJP2());
        context.convert("C:\\Users\\Tarek\\Downloads\\123716_03.tif","C:\\Users\\Tarek\\Downloads/123716_03.jp2",null);


    }
}
