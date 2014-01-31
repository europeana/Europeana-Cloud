package eu.europeana.cloud.client.uis.rest.web;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * Enumeration that holds the relative urls and parameters of each API call
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public enum RelativeUrls {

	/** RelativeUrls CREATECLOUDID */
	CREATECLOUDID {
		@Override
		public List<String> getParamNames() {

			return ImmutableList.of(PROVIDER_ID, RECORD_ID);
		}

		@Override
		public String getUrl() {
			return "createCloudIdLocal";
		}
	},
	/** RelativeUrls CREATECLOUDIDNOLOCAL */
	CREATECLOUDIDNOLOCAL {
		@Override
		public List<String> getParamNames() {
			return ImmutableList.of(PROVIDER_ID);
		}

		@Override
		public String getUrl() {
			return "createCloudIdNoLocal";
		}
	},
	/** RelativeUrls GETCLOUDID */
	GETCLOUDID {
		@Override
		public List<String> getParamNames() {
			return ImmutableList.of(PROVIDER_ID, RECORD_ID);
		}

		@Override
		public String getUrl() {
			return "getCloudId";
		}
	},
	/** RelativeUrls GETLOCALIDS */
	GETLOCALIDS {
		@Override
		public List<String> getParamNames() {
			return ImmutableList.of(CLOUD_ID);
		}

		@Override
		public String getUrl() {
			return "getLocalIds";
		}
	},
	/** RelativeUrls GETLOCALIDSBYPROVIDER */
	GETLOCALIDSBYPROVIDER {
		@Override
		public List<String> getParamNames() {
			return ImmutableList.of(PROVIDER_ID, RECORD_ID, TO);
		}

		@Override
		public String getUrl() {
			return "getLocalIdsByProvider";
		}
	},
	/** RelativeUrls GETCLOUDIDSBYPROVIDER */
	GETCLOUDIDSBYPROVIDER {
		@Override
		public List<String> getParamNames() {
			return ImmutableList.of(PROVIDER_ID, CLOUD_ID, TO);
		}

		@Override
		public String getUrl() {
			return "getCloudIdsByProvider";
		}
	},
	/** RelativeUrls CREATEMAPPING */
	CREATEMAPPING {
		@Override
		public List<String> getParamNames() {
			return ImmutableList.of(CLOUD_ID, PROVIDER_ID, RECORD_ID);
		}

		@Override
		public String getUrl() {
			return "createMapping";
		}
	},
	/** RelativeUrls REMOVEMAPPINGBYLOCALID */
	REMOVEMAPPINGBYLOCALID {
		@Override
		public List<String> getParamNames() {
			return ImmutableList.of(PROVIDER_ID, RECORD_ID);
		}

		@Override
		public String getUrl() {
			return "removeMappingByLocalId";
		}
	},
	/** RelativeUrls DELETECLOUDID */
	DELETECLOUDID {
		@Override
		public List<String> getParamNames() {
			return ImmutableList.of(CLOUD_ID);
		}

		@Override
		public String getUrl() {
			return "deleteCloudId";
		}
	},
	/**
	 * Create provider
	 */
	CREATEPROVIDER {

		@Override
		public List<String> getParamNames() {
			return ImmutableList.of(PROVIDER_ID);
		}

		@Override
		public String getUrl() {
			return "";
		}

	},
	/**
	 * Retrieve provider
	 */
	RETRIEVEPROVIDERS {

		@Override
		public List<String> getParamNames() {
			return ImmutableList.of(STARTFROM);
		}

		@Override
		public String getUrl() {

			return "";
		}

	};

	private static final String PROVIDER_ID = "providerId";
	private static final String RECORD_ID = "recordId";
	private static final String CLOUD_ID = "cloudId";
	private static final String TO = "to";
	private static final String STARTFROM = "startFrom";

	/**
	 * Get API calls parameters
	 * 
	 * @return List with the API call parameters
	 */
	public abstract List<String> getParamNames();

	/**
	 * Get the URL call of each method
	 * 
	 * @return The string to be used for the creation of the full URL
	 */
	public abstract String getUrl();
}
