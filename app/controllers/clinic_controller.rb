class ClinicController < GenericClinicController

  def reports
    @reports = [
      ["Cohort","/cohort_tool/cohort_menu"],
      ["Supervision","/clinic/supervision"],
      ["Data Cleaning Tools", "/report/data_cleaning"],
      ["Stock report","/drug/date_select"]
    ]

    render :template => 'clinic/reports', :layout => 'clinic' 
  end

  def supervision
    @supervision_tools = [["Data that was Updated","summary_of_records_that_were_updated"],
      ["Drug Adherence Level","adherence_histogram_for_all_patients_in_the_quarter"],
      ["Visits by Day", "visits_by_day"],
      ["Non-eligible Patients in Cohort", "non_eligible_patients_in_cohort"]]

    @landing_dashboard = 'clinic_supervision'

    render :template => 'clinic/supervision', :layout => 'clinic' 
  end

  def properties
    @settings = [
      ["Set clinic days","/properties/clinic_days"],
      ["View clinic holidays","/properties/clinic_holidays"],
      ["Set clinic holidays","/properties/set_clinic_holidays"],
      ["Set site code", "/properties/site_code"],
      ["Set appointment limit", "/properties/set_appointment_limit"]
    ]
    render :template => 'clinic/properties', :layout => 'clinic' 
  end

  def administration
    @reports =  [
                  ['/clinic/users','User accounts/settings'],
                  ['/clinic/management','Drug Management'], 
                  ['/clinic/location_management','Location Management']
                ]
    @landing_dashboard = 'clinic_administration'
    render :template => 'clinic/administration', :layout => 'clinic' 
  end

  def data_cleaning_tab
    @reports = [
                 ['Missing Prescriptions' , '/cohort_tool/select?report_type=dispensations_without_prescriptions'],
                 ['Missing Dispensations' , '/cohort_tool/select?report_type=prescriptions_without_dispensations'],
                 ['Multiple Start Reasons' , '/cohort_tool/select?report_type=patients_with_multiple_start_reasons'],
                 ['Out of range ARV number' , '/cohort_tool/select?report_type=out_of_range_arv_number'],
                 ['Data Consistency Check' , '/cohort_tool/select?report_type=data_consistency_check'],
                 ['Incomplete Visits' , '/cohort_tool/select?report_type=incomplete_visit']
               ] 
    render :layout => false
  end

  def properties_tab
    if current_program_location.match(/HIV program/i) or current_program_location.match(/TB program/i)
      @settings = [
        ["Set filing limit","/properties/filing_number"],
        ["Set Clinic Days","/properties/clinic_days"],
        ["View Clinic Holidays","/properties/clinic_holidays"],
        ["Ask Pills remaining at home","/properties/creation?value=ask_pills_remaining_at_home"],
        ["Set Clinic Holidays","/properties/set_clinic_holidays"],
        ["Set Site Code", "/properties/site_code"],
        ["Manage Roles", "/properties/set_role_privileges"],
        ["Staging Format", "/properties/staging_properties"],
        ["Use User Selected Task(s)", "/properties/creation?value=use_user_selected_activities"],
        ["Use Filing Numbers", "/properties/creation?value=use_filing_numbers"],
        ["Confirm patient creation", "/properties/creation?value=confirm_before_creating_a_patient"],
        ["Show Lab Results", "/properties/creation?value=show_lab_results"],
        ["Enter Lab Results", "/properties/creation?value=enter_lab_results"],
        ["Set Appointment Limit", "/properties/set_appointment_limit"],
				    ["Export Cohort Data", "/properties/export_cohort_data"],
        ["Extended Family Planning","/properties/creation?value=extended_family_planning"],
        ["Mailing List", "/properties/mailing_management"],
        ["Activate VL routine check", "/properties/creation?value=activate_viral_load_routine_check"],
        ["Activate Drug Management","/properties/creation?value=activate_drug_management"],
        ["Activate Hypertension Screening","/properties/creation?value=activate_hypertension_enhancement"],
        ["Set Hypertension Screening Age", "/properties/set_htn_age_threshold"],
        ["Set Hypertension BP Thresholds", "/properties/set_htn_bp_thresholds"]
      ]
    else
      @settings = []
    end
    render :layout => false
  end


end
