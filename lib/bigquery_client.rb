# frozen_string_literal: true
require 'google/cloud/bigquery'

def setup_bigquery_dataset
  Google::Cloud::Bigquery.configure do |config|
    config.project_id  = ENV.fetch('GCP_PROJECT_ID', nil)
    config.credentials = "./private_key/#{ENV.fetch('GCP_CREDENTIALS_FILE_NAME', nil)}"
  end
  bigquery = Google::Cloud::Bigquery.new
  bigquery.dataset(ENV.fetch('DATASET_NAME', nil), skip_lookup: true)
end

def dataset
  @dataset ||= setup_bigquery_dataset
end

def table_client(table_name)
  dataset.table(table_name, skip_lookup: true)
end
