package eu.europeana.cloud.client.uis.rest.web;

import java.util.ArrayList;
import java.util.List;

/**
 * Enumeration that holds the relative urls and parameters of each API call
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public enum RelativeUrls {

	CREATECLOUDID {
		@Override
		public List<String> getParamNames() {
			
			return new ArrayList<String>(){/**
				 * 
				 */
				private static final long serialVersionUID = -4917925348535440990L;

			{
				add(PROVIDER_ID);
				add(RECORD_ID);
			}
			};
		}

		@Override
		public String getUrl() {
			return "createCloudIdLocal";
		}
	}, CREATECLOUDIDNOLOCAL {
		@Override
		public List<String> getParamNames() {
			
			return new ArrayList<String>(){/**
				 * 
				 */
				private static final long serialVersionUID = -4917925348535440990L;

			{
				add(PROVIDER_ID);
			}
			};
		}

		@Override
		public String getUrl() {
			return "createCloudIdNoLocal";
		}
	},	GETCLOUDID {
		@Override
		public List<String> getParamNames() {
			return new ArrayList<String>(){/**
				 * 
				 */
				private static final long serialVersionUID = -1592758113004504815L;

			{
				add(PROVIDER_ID);
				add(RECORD_ID);
			}
			};
		}

		@Override
		public String getUrl() {
			return "getCloudId";
		}
	}, GETLOCALIDS {
		@Override
		public List<String> getParamNames() {
			return new ArrayList<String>(){/**
				 * 
				 */
				private static final long serialVersionUID = 3452383847573071930L;

			{
				add(CLOUD_ID);
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
			return new ArrayList<String>(){/**
				 * 
				 */
				private static final long serialVersionUID = 7408709766048773111L;

			{
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
	}, GETCLOUDIDSBYPROVIDER {
		@Override
		public List<String> getParamNames() {
			return new ArrayList<String>(){/**
				 * 
				 */
				private static final long serialVersionUID = 7520663886629247245L;

			{
				add(PROVIDER_ID);
				add(CLOUD_ID);
				add(TO);
			}
			};
		}

		@Override
		public String getUrl() {
			return "getCloudIdsByProvider";
		}
	}, CREATEMAPPING {
		@Override
		public List<String> getParamNames() {
			return new ArrayList<String>(){/**
				 * 
				 */
				private static final long serialVersionUID = 501580880647046185L;

			{
				add(CLOUD_ID);
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
			return new ArrayList<String>(){/**
				 * 
				 */
				private static final long serialVersionUID = 7585573871277473127L;

			{
				add(PROVIDER_ID);
				add(RECORD_ID);
			}
			};
		}

		@Override
		public String getUrl() {
			return "removeMappingByLocalId";
		}
	}, DELETECLOUDID {
		@Override
		public List<String> getParamNames() {
			return new ArrayList<String>(){/**
				 * 
				 */
				private static final long serialVersionUID = 7715313142197545810L;

			{
				add(CLOUD_ID);
			}
			};
		}

		@Override
		public String getUrl() {
			return "deleteCloudId";
		}
	};

	private static final String PROVIDER_ID="providerId";
	private static final String RECORD_ID="recordId";
	private static final String CLOUD_ID="cloudId";
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
