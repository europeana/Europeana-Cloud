
package eu.europeana.cloud.service.mcs.persistent;

class PutResult {

    private final String md5;
    private final Long contentLength;
    
    public PutResult(String md5, Long contentLength) {
        this.md5 = md5;
        this.contentLength = contentLength;
    }

    public String getMd5() {
        return md5;
    }

    public Long getContentLength() {
        return contentLength;
    }
}
