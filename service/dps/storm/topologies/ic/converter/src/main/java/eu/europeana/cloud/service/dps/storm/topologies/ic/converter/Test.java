package eu.europeana.cloud.service.dps.storm.topologies.ic.converter;

import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter.ConverterContext;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter.ImageMagicJPGToTiff;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.converter.KakaduConverterTiffToJP2;

/**
 * Created by Tarek on 4/6/2016.
 */
public class Test {
    public static void main(String[] args) throws Exception {
        ConverterContext context = new ConverterContext(new KakaduConverterTiffToJP2());
        context.convert("c:\\Users\\Tarek\\Desktop\\imageMagicExample\\1.tiff", "c:\\Users\\Tarek\\Desktop\\imageMagicExample\\123716_03.jp2", null);

        ConverterContext context1 = new ConverterContext(new ImageMagicJPGToTiff());
        context1.convert("C:\\Users\\Tarek\\Desktop\\imageMagicExample\\tarek1.jpg", "C:\\Users\\Tarek\\Desktop\\imageMagicExample\\65.tiff", null);


    }
}
