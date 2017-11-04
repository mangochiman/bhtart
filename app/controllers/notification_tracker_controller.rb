class NotificationTrackerController < ApplicationController
  
  def track
    patient_id = session[:active_patient_id]
    notification = NotificationTracker.create_notification(params[:notification_name], params[:response_text], patient_id)  
    render :text => notification.to_json and return
  end

end
