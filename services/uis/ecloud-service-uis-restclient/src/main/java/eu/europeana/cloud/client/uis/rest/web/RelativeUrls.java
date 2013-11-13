package eu.europeana.cloud.client.uis.rest.web;

import java.util.ArrayList;
import java.util.List;

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
				add(GLOBAL_ID);
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

	private final static String PROVIDER_ID="providerId";
	private final static String RECORD_ID="recordId";
	private final static String GLOBAL_ID="globalId";
	private final static String TO = "to";
	
	public abstract List<String> getParamNames();

	public abstract String getUrl();
}
