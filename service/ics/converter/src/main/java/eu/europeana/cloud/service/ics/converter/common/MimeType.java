package eu.europeana.cloud.service.ics.converter.common;

/**
 * Created by Tarek on 8/28/2015.
 */
public enum  MimeType {
    MIME_IMAGE_TIFF("image/tiff"),
    MIME_IMAGE_JP2("image/jp2");

    private String mimeType;

    MimeType(String mimeType) {
        this.mimeType = mimeType;
    }
    public String getValue()
    {
        return  mimeType;
    }


}

