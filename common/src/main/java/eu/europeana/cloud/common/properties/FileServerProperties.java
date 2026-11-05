package eu.europeana.cloud.common.properties;


import lombok.*;

import java.io.Serializable;

/**
 * Describes properties related with connection to File Server for http topology
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FileServerProperties implements Serializable {
    private String locationExternal;
    private String locationInternal;

    public String convertToExternalUrl(String url) {
        return convertUrl(url, locationInternal, locationExternal);
    }

    public String convertToInternalUrl(String url) {
        return convertUrl(url, locationInternal, locationExternal);
    }

    private String convertUrl(String url, String source, String target) {
        if (url.startsWith(source)) {
            return target + url.substring(source.length());
        }

        if (url.startsWith(target)) {
            return url;
        }

        throw new IllegalArgumentException(
                "URL does not start with the expected prefix: " + source
        );
    }
}
