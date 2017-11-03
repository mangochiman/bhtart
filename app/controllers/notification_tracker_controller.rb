class NotificationTrackerController < ApplicationController
  
  def track
    notification = NotificationTracker.create_notification(params[:notification_name], params[:response_text])  
    render :text => notification.to_json and return
  end

end
