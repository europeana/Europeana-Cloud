
package eu.europeana.cloud.service.mcs.persistent;

public class PutResult {

    private String md5;
    private Long contentLength;
    
    public PutResult(String md5, Long contentLength) {
        this.md5 = md5;
        this.contentLength = contentLength;
    }

    
    public void setMd5(String md5) {
        this.md5 = md5;
    }
    
    public String getMd5() {
        return md5;
    }

    public void setContentLength(Long contentLength) {
        this.contentLength = contentLength;
    }

    public Long getContentLength() {
        return contentLength;
    }
}
