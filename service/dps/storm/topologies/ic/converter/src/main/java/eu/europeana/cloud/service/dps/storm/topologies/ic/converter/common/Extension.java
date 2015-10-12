package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.common;

/**
 * Created by Tarek on 8/28/2015.
 */
public enum Extension {
    Tiff(new String[]{"tiff", "tif"}),
    Jp2(new String[]{"jp2"});

    private String[] values;

    Extension(String[] values) {
        this.values = values;
    }

    public String[] getValues() {
        return values;
    }
}

