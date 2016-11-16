class ValidationResult < ActiveRecord::Base
  belongs_to :validation_rule, :foreign_key => "rule_id"

	def self.add_record(data)
    database = "validation_result" #to come from config file
    username = "root" #to come from config file
    password = "password" #to come from config file
    port = "5984" #to come from config file
    url = "http://#{username}:#{password}@localhost:#{port}/#{database}"

    RestClient.post(url, data.to_json, :content_type => "application/json")
  end
end
