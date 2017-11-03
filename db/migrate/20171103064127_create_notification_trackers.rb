class CreateNotificationTrackers < ActiveRecord::Migration
  def self.up
    create_table  :notification_tracker,  :primary_key => :tracker_id do |t|
      t.string    :notification_name,     :null => false
      t.text      :description
      t.string    :notification_response, :null => false
      t.datetime  :notification_datetime, :null => false
      t.integer   :user_id,               :null => false
    end
  end

  def self.down
    drop_table :notification_tracker
  end
end
