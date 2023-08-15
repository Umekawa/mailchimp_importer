
require './lib/mailchimp_client'

def get_method(table_name, num, list_id = nil, subscriber_hash = nil)
  case table_name
  when 'campaigns'
    mailchimp_client.campaigns.list(count: COUNT_NUM, offset: num)
  when 'lists'
    mailchimp_client.lists.get_all_lists(count: COUNT_NUM, offset: num)
  when 'list_members'
    mailchimp_client.lists.get_list_members_info(list_id, count: COUNT_NUM, offset: num)
  when 'member_activities'
    mailchimp_client.lists.get_list_member_activity(list_id, subscriber_hash, count: COUNT_NUM, offset: num)
  end
end

def get_records(table_name, num, list_id, subscriber_hash) # rubocop:disable Metrics/MethodLength
  records = nil
  if table_name.eql?('member_activities')
    @get_records_time += Benchmark.realtime do
      records = get_method(table_name, num, list_id, subscriber_hash)[get_results(table_name)]
    end
  else
    records = get_method(table_name, num, list_id, subscriber_hash)[get_results(table_name)]
  end
  records
rescue MailchimpMarketing::ApiError => e
  puts e.inspect
  nil
end