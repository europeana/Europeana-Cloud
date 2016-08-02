CREATE TABLE data_sets_representations_names(
       provider_id varchar,
       dataset_id varchar,
       representations_names set<text>,
       PRIMARY KEY ((provider_id, dataset_id))
)WITH comment='Retrieve information about the representations supported in a provider’s dataset';