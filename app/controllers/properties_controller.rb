class PropertiesController < GenericPropertiesController
	def export_cohort_data
    if request.post? and not params[:export_cohort_data].blank?
      session["export.cohort.data"] = params[:export_cohort_data]
      
      if params[:view_configuration]
        redirect_to("/clinic/system_configurations") and return
      end

      redirect_to '/clinic' and return
    end
	end

	def staging_options
    global_setting = GlobalProperty.find_by_property("use.extended.staging.questions")
    global_setting.property_value = "false"
    global_setting.property_value = "true" if params['staging_options'].to_s.match(/extended/i)
    global_setting.save

    if params[:view_configuration]
      redirect_to("/clinic/system_configurations") and return
    end
    
    redirect_to '/clinic' and return
	end

  def filing_number
    if request.post?
			filing_limit = GlobalProperty.find_by_property("filing.number.limit") rescue nil
      if filing_limit.nil?
        filing_limit = GlobalProperty.new
        filing_limit.property = "filing.number.limit"
        filing_limit.description = "Maximum number for archiving of files to begin"
        filing_limit.property_value = params[:filing_number]
        filing_limit.save
      else
        filing_limit = GlobalProperty.find_by_property("filing.number.limit")
        filing_limit.property_value = params[:filing_number]
        filing_limit.save
      end

      if params[:view_configuration]
        redirect_to("/clinic/system_configurations") and return
      end
      
      redirect_to '/clinic' and return
    end
  end

  def set_htn_age_threshold

    if request.post?
      age_threshold = GlobalProperty.find_by_property('htn.screening.age.threshold')

      if age_threshold.blank?
        age_threshold = GlobalProperty.new()
        age_threshold.property = "htn.screening.age.threshold"
        age_threshold.description = "Defines the age at which patients will start being screened for hypertension"
      end

      age_threshold.property_value = params[:value]
      age_threshold.save

      if params[:view_configuration]
        redirect_to("/clinic/system_configurations") and return
      end
      
      redirect_to "/clinic" and return
    end
  end

  def set_htn_bp_thresholds
    if request.post?
      diastolic_threshold = GlobalProperty.find_by_property('htn.diastolic.threshold')
      systolic_threshold = GlobalProperty.find_by_property('htn.systolic.threshold')

      if diastolic_threshold.blank?
        diastolic_threshold = GlobalProperty.new()
        diastolic_threshold.property = 'htn.diastolic.threshold'
        diastolic_threshold.description = "Defines the measurement at which diastolic blood pressure is considered high"
      end

      if systolic_threshold.blank?
        systolic_threshold = GlobalProperty.new()
        systolic_threshold.property = 'htn.systolic.threshold'
        systolic_threshold.description = "Defines the measurement at which systolic blood pressure is considered high"
      end

      diastolic_threshold.property_value = params[:diastolic]
      diastolic_threshold.save
      systolic_threshold.property_value = params[:systolic]
      systolic_threshold.save

      if params[:view_configuration]
        redirect_to("/clinic/system_configurations") and return
      end
      
      redirect_to "/clinic" and return
    end
  end
end
