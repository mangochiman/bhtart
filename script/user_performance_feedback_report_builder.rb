User.current = User.first

Session_date = '2017-12-01'.to_date
StartDate = Session_date.strftime('%Y-%m-%d 00:00:00')
EndDate = Session_date.strftime('%Y-%m-%d 23:59:59')

Location.current_location = Location.find(725)

Patient_visit_concept_name = ConceptName.find_by_name('Patient present')
YesConcept = ConceptName.find_by_name('Yes')



@selected_activities = {} 

def notification_tracker_user_activities

	activities = ActiveRecord::Base.connection.select_all <<EOF
	SELECT * FROM notification_tracker_user_activities 
	WHERE login_datetime <= '#{EndDate}';
EOF

	activities.each do |a|
		user_id = a['user_id'].to_i
	  login_datetime = a['login_datetime'].to_time.strftime('%Y-%m-%d %H:%M:%S')	

		if @selected_activities[user_id].blank?
			@selected_activities[user_id] = {}
		end

		if @selected_activities[user_id][login_datetime].blank?
			@selected_activities[user_id][login_datetime] = []
		end

		@selected_activities[user_id][login_datetime] = a['selected_activities'].split('##')
	end unless activities.blank?

end



def start
	notification_tracker_user_activities

  build_temp_tables

	fetch_patient_seen

	fetch_overall_record_complete_status
end

def fetch_overall_record_complete_status
	all_patients = ActiveRecord::Base.connection.select_all <<EOF
		SELECT * FROM patient_seen;
EOF


	(all_patients || []).each do |r|
		patient_id = r['patient_id'].to_i
		visit_date = r['visit_date'].to_date

		#patient_visit = Observation.find(:last, :conditions =>["person_id = ? 
		#	AND obs_datetime BETWEEN ? AND ? AND concept_id = ? AND value_coded = ?",
		#	patient_id, StartDate, EndDate, 
		#	Patient_visit_concept_name.concept_id, YesConcept.concept_id]).blank? != true

		tasks_skipped = check_for_skipped_tasks(patient_id)

		complete = true if tasks_skipped.blank?
		unless tasks_skipped.blank?
			if tasks_skipped.encounter_type.match(/None/i)
				complete = true
			else
				complete = false
			end
		end

		puts "###################################### #{tasks_skipped.encounter_type}"
		unless complete
  		ActiveRecord::Base.connection.execute <<EOF
				INSERT INTO overall_record_complete_status (patient_seen_id, complete)
 				VALUES(#{r['patient_seen_id']}, #{complete})
EOF

		end
		#raise complete.inspect
	end

end

def check_for_skipped_tasks(patient_id)
	missed_encounters = []
	patient = Patient.find(patient_id)
	activities = []

	(@selected_activities[patient_id] || {}).sort_by{|t, a| t.to_time}.reverse.each do |t, a|
		if t.to_time <= EndDate.to_time
			activities = a.split('##')
			break
		end
	end

	task = get_next_form(Location.current_location, patient, StartDate.to_date, activities)
	return task
end

def fetch_patient_seen
	hiv_encounter_types = ['HIV RECEPTION','HIV STAGING','VITALS','PART_FOLLOWUP','HIV CLINIC REGISTRATION',
    'DISPENSING','HIV CLINIC CONSULTATION','TREATMENT','ART ADHERENCE','APPOINTMENT']

	encounter_ids = EncounterType.find(:all, :conditions =>["name IN(?)", hiv_encounter_types]).map(&:id)

  ActiveRecord::Base.connection.execute <<EOF
    INSERT INTO patient_seen (patient_id, visit_date) 
			SELECT DISTINCT(patient_id) patient_id, DATE(encounter_datetime) visit_date FROM
			encounter WHERE encounter_datetime BETWEEN '#{StartDate}'
			AND '#{EndDate}' AND encounter_type IN(#{encounter_ids.join(',')})
			ORDER BY patient_id;
EOF

	
end

def build_temp_tables

  ActiveRecord::Base.connection.execute <<EOF
    DROP TABLE IF EXISTS `patient_seen`;
EOF

  ActiveRecord::Base.connection.execute <<EOF
   CREATE TABLE `patient_seen` (
 		`patient_seen_id` int(11) NOT NULL AUTO_INCREMENT,
 		`patient_id` int(11) NOT NULL,
 		`visit_date` date NOT NULL,
		 PRIMARY KEY (`patient_seen_id`),
 		 UNIQUE KEY `ID_UNIQUE` (`patient_id`)
	 );
EOF

	puts "Created patient_seen ...."


  ActiveRecord::Base.connection.execute <<EOF
    DROP TABLE IF EXISTS `provider_record_complete_status`;
EOF

  ActiveRecord::Base.connection.execute <<EOF
   CREATE TABLE `provider_record_complete_status` (
 		`id` int(11) NOT NULL AUTO_INCREMENT,
 		`patient_seen_id` int(11) NOT NULL,
 		`provider_id` int(11) NOT NULL,
 		`complete` SMALLINT(6) NOT NULL DEFAULT 0,
		 PRIMARY KEY (`id`),
 		 UNIQUE KEY `ID_UNIQUE` (`id`)
	 );
EOF

	puts "Created provider_record_complete_status ...."


  ActiveRecord::Base.connection.execute <<EOF
    DROP TABLE IF EXISTS `overall_record_complete_status`;
EOF

  ActiveRecord::Base.connection.execute <<EOF
   CREATE TABLE `overall_record_complete_status` (
 		`id` int(11) NOT NULL AUTO_INCREMENT,
 		`patient_seen_id` int(11) NOT NULL,
 		`complete` SMALLINT(6) NOT NULL DEFAULT 0,
		 PRIMARY KEY (`id`),
 		 UNIQUE KEY `ID_UNIQUE` (`id`)
	 );
EOF

	puts "Created overall_record_complete_status ...."


end



#/////////////////////////////////////////////////////////////////////


def get_next_form(location , patient , session_date, activities)
  current_user = User.current
  current_user_activities = activities
	current_user_roles = []

	task = Task.new()
=begin
  if current_user_activities.blank?
    task.encounter_type = "NO TASKS SELECTED"
    task.url = "/patients/show/#{patient.id}"
    return task
  end
=end

  pp = PatientProgram.find(:first, :joins => :location, 
    :conditions => ["program_id = ? AND patient_id = ?", 
    Program.find_by_concept_id(Concept.find_by_name('HIV PROGRAM').id).id,
    patient.id]).patient_states.last.program_workflow_state.concept.fullname	rescue ""

  current_day_encounters = Encounter.find(:all,
    :conditions =>["patient_id = ? AND DATE(encounter_datetime) = ?",
      patient.id,session_date.to_date]).map{|e|e.name.upcase}

  #we get the sequence of clinic questions(encounters) form the GlobalProperty table
  #property: list.of.clinical.encounters.sequentially
  #property_value: ?

  #valid privileges for ART visit ....
  #1. Manage Vitals - VITALS
  #2. Manage pre ART visits - PART_FOLLOWUP
  #3. Manage HIV staging visits - HIV STAGING
  #4. Manage HIV reception visits - HIV RECEPTION
  #5. Manage HIV first visit - HIV CLINIC REGISTRATION
  #6. Manage drug dispensations - DISPENSING
  #7. Manage HIV clinic consultations - HIV CLINIC CONSULTATION
  #8. Manage TB reception visits -?
  #9. Manage prescriptions - TREATMENT
  #10. Manage appointments - APPOINTMENT
  #11. Manage ART adherence - ART ADHERENCE
  hiv_program_status = Patient.find_by_sql("
      SELECT patient_id, current_state_for_program(#{patient_id}, 1, '#{session_date}') AS state, c.name as status
      FROM patient p INNER JOIN program_workflow_state pw ON pw.program_workflow_state_id = current_state_for_program(#{patient_id}, 1, '#{session_date}')
      INNER JOIN concept_name c ON c.concept_id = pw.concept_id where p.patient_id = '#{patient.id}'").first.status rescue "Unknown"


  encounters_sequentially = CoreService.get_global_property_value('list.of.clinical.encounters.sequentially')

  encounters = encounters_sequentially.split(',')

  user_selected_activities = current_user.activities.collect{|a| a.upcase }.join(',') rescue []
  if encounters.blank? or user_selected_activities.blank?
    task.url = "/patients/show/#{patient.id}"
    return task
  end
  ############ FAST TRACK #################
  #fast_track_patient = false
  #latest_fast_track_answer = patient.person.observations.recent(1).question("FAST").first.answer_string.squish.upcase rescue nil
  #fast_track_patient = true if latest_fast_track_answer == 'YES'

  #fast_track_patient = false if (tb_suspected_or_confirmed?(patient, session_date) == true)
  #fast_track_patient = false if (is_patient_on_htn_treatment?(patient, session_date) == true)

  if (fast_track_patient?(patient, session_date) || fast_track_done_today(patient, session_date))
    #fast_track_done_today method: this is to for tracking a patient if the patient is a fast track after even the visit is done on that day
    return fast_track_next_form(location , patient , session_date, activities)
  end
  ########### FAST TRACK END ##############

  art_reason = patient.person.observations.recent(1).question("REASON FOR ART ELIGIBILITY").all rescue nil
  reason_for_art = PatientService.reason_for_art_eligibility(patient)
  if not reason_for_art.blank? and reason_for_art.upcase == 'NONE'
    reason_for_art = nil
  end
  #raise encounters.to_yaml
  (encounters || []).each do | type |
    type =type.squish
    next if pp.match(/patient\sdied/i)
    encounter_available = Encounter.find(:first,:conditions =>["patient_id = ? AND encounter_type = ? AND DATE(encounter_datetime) = ?",
        patient.id,EncounterType.find_by_name(type).id, session_date],
      :order =>'encounter_datetime DESC,date_created DESC',:limit => 1) rescue nil

    # next if encounter_available.nil?

    reception = Encounter.find(:first,:conditions =>["patient_id = ? AND DATE(encounter_datetime) = ? AND encounter_type = ?",
        patient.id,session_date,EncounterType.find_by_name('HIV RECEPTION').id]).observations.collect{| r | r.to_s}.join(',') rescue ''

    task.encounter_type = type
    case type
    when 'VITALS'
      if encounter_available.blank? and user_selected_activities.match(/Manage Vitals/i)
        task.url = "/encounters/new/vitals?patient_id=#{patient.id}"
        return task
      elsif encounter_available.blank? and not user_selected_activities.match(/Manage Vitals/i)
        task.url = "/patients/show/#{patient.id}"
        return task
      end if reception.match(/PATIENT PRESENT FOR CONSULTATION:  YES/i)
    when 'HIV CLINIC CONSULTATION'
      unless encounter_available.blank? and user_selected_activities.match(/Manage HIV clinic consultations/i)
        if (patient_has_stopped_fast_track_at_adherence?(patient, session_date))
          consultation_encounters_count = todays_consultation_encounters(patient, session_date)
          if (consultation_encounters_count == 1)
            task.url = "/encounters/new/hiv_clinic_consultation?patient_id=#{patient.id}"
            return task
          end
        end
      end

      if encounter_available.blank? and user_selected_activities.match(/Manage HIV clinic consultations/i)
        task.url = "/encounters/new/hiv_clinic_consultation?patient_id=#{patient.id}"
        return task
      elsif encounter_available.blank? and not user_selected_activities.match(/Manage HIV clinic consultations/i)
        task.url = "/patients/show/#{patient.id}"
        return task
      end

      #if a nurse has refered a patient to a doctor/clinic
      concept_id = ConceptName.find_by_name("Refer to ART clinician").concept_id
      ob = Observation.find(:first,:conditions =>["person_id = ? AND concept_id = ? AND obs_datetime >= ? AND obs_datetime <= ?",
          patient.id, concept_id, session_date, session_date.to_s + ' 23:59:59'],:order =>"obs_datetime DESC, date_created DESC")

      refer_to_clinician = ob.to_s.squish.upcase == 'Refer to ART clinician: yes'.upcase


      if current_user_roles.include?('Nurse')
        adherence_encounter_available = Encounter.find(:first,
          :conditions =>["patient_id = ? AND encounter_type = ? AND DATE(encounter_datetime) = ?",
            patient.id,EncounterType.find_by_name("ART ADHERENCE").id,session_date],
          :order =>'encounter_datetime DESC,date_created DESC',:limit => 1)

        arv_drugs_given = false
        MedicationService.art_drug_given_before(patient,session_date).each do |order|
          arv_drugs_given = true
          break
        end

        if arv_drugs_given
          if adherence_encounter_available.blank? and user_selected_activities.match(/Manage ART adherence/i)
            task.encounter_type = "ART ADHERENCE"
            task.url = "/encounters/new/art_adherence?show&patient_id=#{patient.id}"
            return task
          elsif adherence_encounter_available.blank? and not user_selected_activities.match(/Manage ART adherence/i)
            task.encounter_type = "ART ADHERENCE"
            task.url = "/patients/show/#{patient.id}"
            return task
          end if not MedicationService.art_drug_given_before(patient,session_date).blank?
        end
      end if refer_to_clinician


      if not encounter_available.blank? and refer_to_clinician
        task.url = "/patients/show/#{patient.id}"
        task.encounter_type = task.encounter_type + " (Clinician)"
        return task
      end if current_user_roles.include?('Nurse')

      roles = current_user_roles.join(',') rescue ''
      clinician_or_doctor = roles.match(/Clinician/i) or roles.match(/Doctor/i)

      if not encounter_available.blank? and refer_to_clinician

        if user_selected_activities.match(/Manage HIV clinic consultations/i)
          task.url = "/encounters/new/hiv_clinic_consultation?patient_id=#{patient.id}"
          task.encounter_type = task.encounter_type + " (Clinician)"
          return task
        elsif not user_selected_activities.match(/Manage HIV clinic consultations/i)
          task.url = "/patients/show/#{patient.id}"
          task.encounter_type = task.encounter_type + " (Clinician)"
          return task
        end
      end if clinician_or_doctor
    when 'HIV STAGING'
      next unless reason_for_art.blank?
      staging = session["#{patient.id}"]["#{session_date.to_date}"][:stage_patient] rescue []
      if ! staging.blank?
        next if staging == "No"
      end
      arv_drugs_given = false
      MedicationService.drug_given_before(patient,session_date).each do |order|
        next unless MedicationService.arv(order.drug_order.drug)
        arv_drugs_given = true
      end
      next if arv_drugs_given
      if encounter_available.blank? and user_selected_activities.match(/Manage HIV staging visits/i)
        task.url = "/encounters/new/hiv_staging?show&patient_id=#{patient.id}"
        return task
      elsif encounter_available.blank? and not user_selected_activities.match(/Manage HIV staging visits/i)
        task.url = "/patients/show/#{patient.id}"
        return task
      end if reason_for_art.nil? or reason_for_art.blank? or hiv_program_status == "Pre-ART (Continue)"
    when 'HIV RECEPTION'
      encounter_hiv_clinic_registration = Encounter.find(:first,:conditions =>["patient_id = ? AND encounter_type = ?",
          patient.id,EncounterType.find_by_name('HIV CLINIC REGISTRATION').id],
        :order =>'encounter_datetime DESC',:limit => 1)
      transfer_in = encounter_hiv_clinic_registration.observations.collect{|r|r.to_s.strip.upcase}.include?('HAS TRANSFER LETTER: YES'.upcase) rescue []
      hiv_staging = Encounter.find(:first,:conditions =>["patient_id = ? AND encounter_type = ?",
          patient.id,EncounterType.find_by_name('HIV STAGING').id],:order => "encounter_datetime DESC")

      if transfer_in and hiv_staging.blank? and user_selected_activities.match(/Manage HIV first visits/i)
        task.url = "/encounters/new/hiv_staging?show&patient_id=#{patient.id}"
        task.encounter_type = 'HIV STAGING'
        return task
      elsif encounter_available.blank? and user_selected_activities.match(/Manage HIV reception visits/i)
        task.url = "/encounters/new/hiv_reception?show&patient_id=#{patient.id}"
        return task
      elsif encounter_available.blank? and not user_selected_activities.match(/Manage HIV reception visits/i)
        task.url = "/patients/show/#{patient.id}"
        return task
      end
    when 'HIV CLINIC REGISTRATION'
      #encounter_hiv_clinic_registration = Encounter.find(:first,:conditions =>["patient_id = ? AND encounter_type = ?",
      #                              patient.id,EncounterType.find_by_name(type).id],
      #                             :order =>'encounter_datetime DESC',:limit => 1)

      hiv_clinic_registration = require_hiv_clinic_registration(patient)

      if hiv_clinic_registration and user_selected_activities.match(/Manage HIV first visits/i)
        task.url = "/encounters/new/hiv_clinic_registration?show&patient_id=#{patient.id}"
        return task
      elsif hiv_clinic_registration and not user_selected_activities.match(/Manage HIV first visits/i)
        task.url = "/patients/show/#{patient.id}"
        return task
      end
    when 'DISPENSING'
      encounter_hiv_clinic_consultation = Encounter.find(:first,:conditions =>["patient_id = ? AND encounter_type = ? AND DATE(encounter_datetime) = ?",
          patient.id,EncounterType.find_by_name('HIV CLINIC CONSULTATION').id,session_date],
        :order =>'encounter_datetime DESC,date_created DESC',:limit => 1)
      next unless encounter_hiv_clinic_consultation.observations.map{|obs| obs.to_s.strip.upcase }.include? 'Prescribe drugs:  Yes'.upcase

      treatment = Encounter.find(:first,:conditions =>["patient_id = ? AND DATE(encounter_datetime) = ? AND encounter_type = ?",
          patient.id,session_date,EncounterType.find_by_name('TREATMENT').id])

      if encounter_available.blank? and user_selected_activities.match(/Manage drug dispensations/i)
        task.url = "/patients/treatment_dashboard/#{patient.id}"
        return task
      elsif encounter_available.blank? and not user_selected_activities.match(/Manage drug dispensations/i)
        task.url = "/patients/show/#{patient.id}"
        return task
      end if not treatment.blank?
    when 'TREATMENT'
      encounter_hiv_clinic_consultation = Encounter.find(:first,:conditions =>["patient_id = ? AND encounter_type = ? AND DATE(encounter_datetime) = ?",
          patient.id,EncounterType.find_by_name('HIV CLINIC CONSULTATION').id,session_date],
        :order =>'encounter_datetime DESC,date_created DESC',:limit => 1)

      concept_id = ConceptName.find_by_name("Prescribe drugs").concept_id
      ob = Observation.find(:first,:conditions =>["person_id=? AND concept_id =?",
          patient.id,concept_id],:order =>"obs_datetime DESC,date_created DESC")

      prescribe_arvs = ob.to_s.squish.upcase == 'Prescribe drugs: Yes'.upcase

      concept_id = ConceptName.find_by_name("Refer to ART clinician").concept_id
      ob = Observation.find(:first,:conditions =>["person_id=? AND concept_id =?",
          patient.id,concept_id],:order =>"obs_datetime DESC,date_created DESC")

      not_refer_to_clinician = ob.to_s.squish.upcase == 'Refer to ART clinician: No'.upcase

      if prescribe_arvs and not_refer_to_clinician
        show_treatment = true
      else
        show_treatment = false
      end

      if encounter_available.blank? and user_selected_activities.match(/Manage prescriptions/i)
        task.url = "/regimens/new?patient_id=#{patient.id}"
        return task
      elsif encounter_available.blank? and not user_selected_activities.match(/Manage prescriptions/i)
        task.url = "/patients/show/#{patient.id}"
        return task
      end if show_treatment
    when 'ART ADHERENCE'
      arv_drugs_given = false
      MedicationService.art_drug_given_before(patient,session_date).each do |order|
        arv_drugs_given = true
        break
      end

      MedicationService.art_drug_prescribed_before(patient,session_date).each do |order|
        arv_drugs_given = true
        break
      end unless arv_drugs_given

      next unless arv_drugs_given

      if arv_drugs_given
        if encounter_available.blank? and user_selected_activities.match(/Manage ART adherence/i)
          task.url = "/encounters/new/art_adherence?show&patient_id=#{patient.id}"
          return task
        elsif encounter_available.blank? and not user_selected_activities.match(/Manage ART adherence/i)
          task.url = "/patients/show/#{patient.id}"
          return task
        end
      end
    end
  end
  #task.encounter_type = 'Visit complete ...'
  task.encounter_type = 'NONE'
  task.url = "/patients/show/#{patient.id}"
  return task
end


##############################################################
def fast_track_next_form(location , patient , session_date, activities)
	task = (Task.first.nil?)? Task.new() : Task.first
	user_selected_activities = activities.collect{|a| a.upcase }.join(',') rescue []

	concept_id = ConceptName.find_by_name("Prescribe drugs").concept_id
	prescribe_drugs_question = Observation.find(:first,:conditions =>["person_id=? AND concept_id =? AND
						DATE(obs_datetime) =?", patient.id, concept_id, session_date])

	hiv_reception_enc = Encounter.find(:first,:conditions =>["patient_id = ? AND DATE(encounter_datetime) = ? AND
				encounter_type = ?", patient.id, session_date, EncounterType.find_by_name('HIV RECEPTION').id])

	adherence_enc = Encounter.find(:first,:conditions =>["patient_id = ? AND DATE(encounter_datetime) = ? AND encounter_type = ?",
			patient.id, session_date, EncounterType.find_by_name('ART ADHERENCE').id])

	treatment_enc = Encounter.find(:first,:conditions =>["patient_id = ? AND DATE(encounter_datetime) = ? AND encounter_type = ?",
			patient.id, session_date, EncounterType.find_by_name('TREATMENT').id])

	dispensation_enc = Encounter.find(:first,:conditions =>["patient_id = ? AND DATE(encounter_datetime) = ? AND encounter_type = ?",
			patient.id, session_date, EncounterType.find_by_name('DISPENSING').id])

	if hiv_reception_enc.blank?
		if (user_selected_activities.match(/Manage HIV reception visits/i))
			task.encounter_type = "HIV RECEPTION"
			task.url = "/encounters/new/hiv_reception?patient_id=#{patient.id}"
			return task
		else
			task.url = "/patients/show/#{patient.id}"
			return task
		end
	end

	if (adherence_enc.blank? )
		if (user_selected_activities.match(/Manage ART adherence/i))
			task.encounter_type = "ART ADHERENCE"
			task.url = "/encounters/new/art_adherence?patient_id=#{patient.id}"
			return task
		else
			task.url = "/patients/show/#{patient.id}"
			return task
		end
	end if not MedicationService.art_drug_given_before(patient,session_date).blank?

	unless prescribe_drugs_question.blank?

		if (prescribe_drugs_question.answer_string.squish.upcase == 'YES')
			if (treatment_enc.blank?)
				if (user_selected_activities.match(/Manage prescriptions/i))
					task.encounter_type = "TREATMENT"
					task.url = "/regimens/new?patient_id=#{patient.id}"
					return task
				else
					task.url = "/patients/show/#{patient.id}"
					return task
				end
			end

			if dispensation_enc.blank?
				if (user_selected_activities.match(/Manage drug dispensations/i))
					task.encounter_type = "DISPENSING"
					task.url = "/patients/treatment_dashboard/#{patient.id}"
					return task
				else
					task.url = "/patients/show/#{patient.id}"
					return task
				end
			end unless treatment_enc.blank?

		end

	end

	task.encounter_type = "NONE"
	task.url = "/patients/show/#{patient.id}"
	return task
end

def fast_track_patient?(patient, session_date)
	fast_track_patient = false
	latest_fast_track_answer = patient.person.observations.recent(1).question("FAST").first.answer_string.squish.upcase rescue nil
	fast_track_patient = true if latest_fast_track_answer == 'YES'

	if (patient_has_visited_on_scheduled_date(patient,  session_date) == false)
		fast_track_patient = false
	end

	return fast_track_patient
end

def patient_has_visited_on_scheduled_date(patient,  session_date)
	appointment_date_concept_id = Concept.find_by_name("APPOINTMENT DATE").concept_id
	latest_appointment_date = patient.person.observations.find(:last, :conditions => ["DATE(obs_datetime) < ? AND concept_id =?",
			session_date, appointment_date_concept_id]
	).answer_string.squish.to_date rescue nil

	if (latest_appointment_date.class == Date) #check if it is a valid date object
		min_valid_date = latest_appointment_date - 7.days #One week earlier
		max_valid_date = latest_appointment_date + 7.days #One week later
		if (session_date < min_valid_date || session_date > max_valid_date)
			#The patient came one or more weeks earlier than the appointment date
			#The patient came one or more weeks later than the appointment date
			return false
		end
		return true
	end

	return false
end

def fast_track_done_today(patient, session_date)
	fast_track_done_obs = patient.person.observations.find(:last, :joins => [:encounter],
		:conditions => ["DATE(obs_datetime) = ? AND comments =?", session_date, 'fast track done'])
	return false if fast_track_done_obs.blank?
	return true
end

def require_hiv_clinic_registration(patient_obj = nil)
	require_registration = false
	patient = patient_obj || find_patient

	hiv_clinic_registration = Encounter.find(:first,:conditions =>["patient_id = ?
								AND encounter_type = ?",patient.id,
			EncounterType.find_by_name("HIV CLINIC REGISTRATION").id],
		:order =>'encounter_datetime DESC,date_created DESC')

	require_registration = true if hiv_clinic_registration.blank?

	if !require_registration
		session_date = session[:datetime].to_date rescue Date.today

		current_outcome = latest_state(patient, session_date) || ""

		on_art_before = has_patient_been_on_art_before(patient)

		if current_outcome.match(/Transferred out/i)
			#if on_art_before
			require_registration = false
			#else
			#	require_registration = true
			#end
		end
	end
	return require_registration
end

def latest_state(patient_obj,visit_date)
	program_id = Program.find_by_name('HIV PROGRAM').id
	patient_state = PatientState.find(:first,
		:joins => "INNER JOIN patient_program p
		 ON p.patient_program_id = patient_state.patient_program_id",
		:conditions =>["patient_state.voided = 0 AND p.voided = 0
		 AND p.program_id = ? AND start_date <= ? AND p.patient_id =?",
			program_id,visit_date.to_date,patient_obj.id],
		:order => "start_date DESC, date_created DESC")

	return if patient_state.blank?
	ConceptName.find_by_concept_id(patient_state.program_workflow_state.concept_id).name
end

def has_patient_been_on_art_before(patient)
	on_art = false
	patient_states = PatientProgram.find(:first, :conditions => ["program_id = ? AND location_id = ? AND patient_id = ?",
		Program.find_by_concept_id(Concept.find_by_name('HIV PROGRAM').id).id,
		Location.current_health_center,patient.id]).patient_states rescue []

	(patient_states || []).each do |state|
		if state.program_workflow_state.concept.fullname.match(/antiretrovirals/i)
			on_art = true
		end
	end
	return on_art
end

def patient_has_stopped_fast_track_at_adherence?(patient, session_date)
	stop_reason_concept_id = Concept.find_by_name('STOP REASON').concept_id
	fast_track_stop_reason_obs = patient.person.observations.find(:last, :conditions => ["DATE(obs_datetime) = ? AND
			concept_id =?", session_date, stop_reason_concept_id]
	)
	return false if fast_track_stop_reason_obs.blank?
	encounter_type = fast_track_stop_reason_obs.encounter.type.name rescue nil
	return false if encounter_type.blank?
	return true if encounter_type.match(/ADHERENCE/i)
	return false
end


start
