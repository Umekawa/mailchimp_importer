# frozen_string_literal: true

require 'dotenv'
require 'benchmark'
require 'json'
require './lib/bigquery_client'
require './lib/get_records'

$stdout.sync = true
Dotenv.load

COUNT_NUM = ENV.fetch('COUNT_NUM', 1000).to_i
@get_records_time = 0
@insert_records_time = 0
@record_num = 0
@call_num = 0

def member_list_ids
  @member_list_ids ||= JSON.parse(ENV.fetch('MEMBERS_LIST_IDS', nil))
end

def member_activities_list_ids
  @member_activities_list_ids ||= JSON.parse(ENV.fetch('MEMBER_ACTIVITIES_LIST_IDS', nil))
end

def set_param(record, param)
  case param['type']
  when 'integer'
    record[param['name']]&.to_i
  when 'json'
    record[param['name']].to_json
  when 'string'
    record[param['name']].to_s
  else
    record[param['name']]
  end
end

def get_results(table_name)
  case table_name
  when 'list_members'
    'members'
  when 'member_activities'
    'activity'
  else
    table_name
  end
end

def addtional_params(params, table_name, list_id, subscriber_hash)
  if table_name.eql?('member_activities')
    params['list_id'] = list_id
    params['email_id'] = subscriber_hash
  end
  params['created_at'] = Time.now.utc.strftime('%Y-%m-%d %H:%M:%S')
  params
end

def create_params(table_name, schema, record, list_id, subscriber_hash)
  params = {}
  schema.each do |param|
    next if record[param['name']].eql?(nil)
    next if record[param['name']].eql?('') && param['mode'].eql?('nullable')

    params[param['name']] = set_param(record, param)
  end
  addtional_params(params, table_name, list_id, subscriber_hash)
end

def add_list_members(records, list_id)
  records.each do |record|
    @list_members.push([list_id:, subscriber_hash: record['id']])
  end
end

def create_records(table_name, schema, result, list_id, subscriber_hash)
  result.map do |record|
    create_params(table_name, schema, record, list_id, subscriber_hash)
  end.compact
end

def load_schema(table_name)
  JSON.parse(File.read("schema/#{table_name}.json"))
end

def insert_records(records, table)
  @insert_records_time += Benchmark.realtime do
    table.insert records
  end

  sleep 0.1
end

def transfer_table_data(table_name, table, list_id = nil, subscriber_hash = nil) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength
  @num = 0
  loop do
    result = get_records(table_name, num, list_id, subscriber_hash)
    break if result.nil? || result.size.eql?(0)

    records = create_records(table_name, load_schema(table_name), result, list_id, subscriber_hash)
    insert_records(records, table) if table_name.eql?('member_activities')
    puts result.size if table_name.eql?('list_members')

    @record_num += result.size if table_name.eql?('member_activities')
    @call_num += 1 if table_name.eql?('member_activities')
    num += result.size
    break unless result.size.eql?(COUNT_NUM)

    next unless table_name.eql?('list_members') && member_activities_list_ids.include?(list_id)

    records.each do |record|
      transfer_table_data('member_activities', table_client('member_activities'), list_id, record['id'])
    end
  end
end

def main
  # transfer_table_data('campaigns', table_client('campaigns'))
  # transfer_table_data('lists', table_client('lists'))
  member_list_ids.each do |list_id|
    transfer_table_data('list_members', table_client('list_members'), list_id)
  end
end
main_time = Benchmark.realtime do
  main
end
puts "main_time: #{main_time}"
puts "get_records_time: #{@get_records_time}"
puts "insert_records_time: #{@insert_records_time}"
puts "record_num: #{@record_num}"
puts "call_num: #{@call_num}"