package eu.europeana.cloud.client.uis.rest.web;

import java.util.ArrayList;
import java.util.List;

/**
 * Enumeration that holds the relative urls and parameters of each API call
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public enum RelativeUrls {

	CREATERECORDID {
		@Override
		public List<String> getParamNames() {
			
			return new ArrayList<String>(){{
				add(PROVIDER_ID);
				add(RECORD_ID);
			}
			};
		}

		@Override
		public String getUrl() {
			return "createRecordId";
		}
	}, GETGLOBALID {
		@Override
		public List<String> getParamNames() {
			return new ArrayList<String>(){{
				add(PROVIDER_ID);
				add(RECORD_ID);
			}
			};
		}

		@Override
		public String getUrl() {
			return "getGlobalId";
		}
	}, GETLOCALIDS {
		@Override
		public List<String> getParamNames() {
			return new ArrayList<String>(){{
				add(GLOBAL_ID);
			}
			};
		}

		@Override
		public String getUrl() {
			return "getLocalIds";
		}
	}, GETLOCALIDSBYPROVIDER {
		@Override
		public List<String> getParamNames() {
			return new ArrayList<String>(){{
				add(PROVIDER_ID);
				add(RECORD_ID);
				add(TO);
			}
			};
		}

		@Override
		public String getUrl() {
			return "getLocalIdsByProvider";
		}
	}, GETGLOBALIDSBYPROVIDER {
		@Override
		public List<String> getParamNames() {
			return new ArrayList<String>(){{
				add(PROVIDER_ID);
				add(GLOBAL_ID);
				add(TO);
			}
			};
		}

		@Override
		public String getUrl() {
			return "getGlobalIdsByProvider";
		}
	}, CREATEMAPPING {
		@Override
		public List<String> getParamNames() {
			return new ArrayList<String>(){{
				add(GLOBAL_ID);
				add(PROVIDER_ID);
				add(RECORD_ID);
			}
			};
		}

		@Override
		public String getUrl() {
			return "createMapping";
		}
	}, REMOVEMAPPINGBYLOCALID {
		@Override
		public List<String> getParamNames() {
			return new ArrayList<String>(){{
				add(PROVIDER_ID);
				add(RECORD_ID);
			}
			};
		}

		@Override
		public String getUrl() {
			return "removeMappingByLocalId";
		}
	}, DELETEGLOBALID {
		@Override
		public List<String> getParamNames() {
			return new ArrayList<String>(){{
				add(GLOBAL_ID);
			}
			};
		}

		@Override
		public String getUrl() {
			return "deleteGlobalId";
		}
	};

	private static final String PROVIDER_ID="providerId";
	private static final String RECORD_ID="recordId";
	private static final String GLOBAL_ID="globalId";
	private static final String TO = "to";
	
	/**
	 * Get API calls parameters
	 * @return List with the API call parameters
	 */
	public abstract List<String> getParamNames();

	/**
	 * Get the URL call of each method
	 * @return The string to be used for the creation of the full URL
	 */
	public abstract String getUrl();
}
