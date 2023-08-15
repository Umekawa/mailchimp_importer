# frozen_string_literal: true
require 'MailchimpMarketing'

def setup_mailchimp_client
  client = MailchimpMarketing::Client.new
  client.set_config({
                      api_key: ENV.fetch('MAILCHIMP_API_KEY', nil),
                      server: ENV.fetch('MAILCHIMP_SERVER_PREFIX', nil)
                    })
  client
end

def mailchimp_client
  @mailchimp_client ||= setup_mailchimp_client
end
