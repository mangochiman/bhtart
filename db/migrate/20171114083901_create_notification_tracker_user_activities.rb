class CreateNotificationTrackerUserActivities < ActiveRecord::Migration
  def self.up
    create_table  :notification_tracker_user_activities do |t|
      t.integer   :user_id,               :null => false
      t.datetime  :login_datetime,        :null => false
      t.text      :selected_activities
    end
  end

  def self.down
    drop_table :notification_tracker_user_activities
  end
end
