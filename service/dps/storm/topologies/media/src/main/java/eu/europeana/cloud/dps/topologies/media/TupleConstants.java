package eu.europeana.cloud.dps.topologies.media;

public interface TupleConstants {
	enum UrlType {
		OBJECT("edm:object"),
		HAS_VIEW("edm:hasView"),
		IS_SHOWN_BY("edm:isShownBy"),
		IS_SHOWN_AT("edm:isShownAt");
		
		public final String tagName;
		
		UrlType(String tagName) {
			this.tagName = tagName;
		}
	}
	
	/** Type of the resource file URL. Value type: {@link UrlType} */
	String URL_TYPE = "mediaTopology.urlType";
	
	/** Resource file URL. Value type: {@link String} */
	String URL = "mediaTopology.url";
}
