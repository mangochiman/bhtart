
User.current = User.find(1)
=begin
ART tables in OpenMRS
1.  encounter
2.  obs
3.  orders
4.  drug_orders
6.  patient
7.  person
8.  person_name
9.  person_address
10. person_attribute
11. patient_identifier
12. patient_program
13. patient_state
14. program_workflow
15. program_workflow_state
16. relationship
=end

UniqPatientIds = []
Connection = ActiveRecord::Base.connection
Current_date = "2016-03-25 12:00:52".to_date #Date.today

def start
  patient_ids_in_earliest_start_date = get_earliest_start_date_patients

=begin
  Getting all changed rows in encounter table
=end

  if patient_ids_in_earliest_start_date.blank?
    puts "No changes found on #{Current_date.strftime('%A, %d %B %Y')}" 
    return
  end

  puts "Getting all changed rows in encounter table"

  enc_patient_ids = Connection.select_all("
    SELECT patient_id FROM encounter e
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_changed) = '#{Current_date}' OR 
    DATE(date_voided) = '#{Current_date}') 
    AND patient_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY patient_id
  ").collect{|p|p["patient_id"].to_i}

=begin
  Getting all changed rows in obs table
=end

  puts "Getting all changed rows in obs table"

  obs_patient_ids = Connection.select_all("
    SELECT person_id FROM obs
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_voided) = '#{Current_date}') 
    AND person_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY person_id
  ").collect{|p|p["person_id"].to_i}

=begin
  Getting all changed rows in orders table
=end
  
  orders_patient_ids = Connection.select_all("
    SELECT patient_id FROM orders
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_voided) = '#{Current_date}') 
    AND patient_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY patient_id
  ").collect{|p|p["patient_id"].to_i}

=begin
  Getting all changed rows in patient table
=end

  patient_patient_ids = Connection.select_all("
    SELECT patient_id FROM patient
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_voided) = '#{Current_date}') 
    AND patient_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY patient_id
  ").collect{|p|p["patient_id"].to_i}

=begin
  Getting all changed rows in person table
=end  

  person_patient_ids = Connection.select_all("
    SELECT person_id FROM person
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_changed) = '#{Current_date}' OR 
    DATE(date_voided) = '#{Current_date}') 
    AND person_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY person_id
  ").collect{|p|p["person_id"].to_i}

=begin
  Getting all changed rows in person_name table
=end    

  person_name_patient_ids = Connection.select_all("
    SELECT person_id FROM person_name
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_changed) = '#{Current_date}' OR 
    DATE(date_voided) = '#{Current_date}') 
    AND person_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY person_id
  ").collect{|p|p["person_id"].to_i}

=begin
  Getting all changed rows in person_address table
=end    

  person_address_patient_ids = Connection.select_all("
    SELECT person_id FROM person_address
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_voided) = '#{Current_date}') 
    AND person_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY person_id
  ").collect{|p|p["person_id"].to_i}

=begin
  Getting all changed rows in person_attribute table
=end    
   person_attribute_patient_ids = Connection.select_all("
    SELECT person_id FROM person_attribute
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_changed) = '#{Current_date}' OR 
    DATE(date_voided) = '#{Current_date}') 
    AND person_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY person_id
  ").collect{|p|p["person_id"].to_i}

=begin
  Getting all changed rows in patient_identifier table
=end    

  patient_identifier_patient_ids = Connection.select_all("
    SELECT patient_id FROM patient_identifier
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_voided) = '#{Current_date}') 
    AND patient_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY patient_id
  ").collect{|p|p["patient_id"].to_i}

=begin
  Getting all changed rows in patient_program table
=end    

  patient_program_patient_ids = Connection.select_all("
    SELECT patient_id FROM patient_program
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_changed) = '#{Current_date}' OR 
    DATE(date_voided) = '#{Current_date}') 
    AND patient_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY patient_id
  ").collect{|p|p["patient_id"].to_i}

=begin
  Getting all changed rows in patient_state table
=end    

  patient_state_patient_ids = Connection.select_all("
    SELECT p.patient_id FROM patient_program p 
    inner join patient_state s ON s.patient_program_id = p.patient_program_id
    where (date(s.date_created)= '#{Current_date}' OR date(s.date_changed)='#{Current_date}')
    AND p.patient_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    group by p.patient_id
  ").collect{|p|p["p.patient_id"].to_i}

  puts "Getting all changed rows in orders table"

=begin
  Getting all changed rows in patient table
=end

  puts "Getting all changed rows in patient table"

  patient_patient_ids = Connection.select_all("
    SELECT patient_id FROM patient
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_voided) = '#{Current_date}') 
    AND patient_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY patient_id
  ").collect{|p|p["patient_id"].to_i}

=begin
  Getting all changed rows in person table
=end  

  puts "Getting all changed rows in person table"

  person_patient_ids = Connection.select_all("
    SELECT person_id FROM person
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_changed) = '#{Current_date}' OR 
    DATE(date_voided) = '#{Current_date}') 
    AND person_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY person_id
  ").collect{|p|p["person_id"].to_i}

=begin
  Getting all changed rows in person_name table
=end    

  puts "Getting all changed rows in person_name table"

  person_name_patient_ids = Connection.select_all("
    SELECT person_id FROM person_name
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_changed) = '#{Current_date}' OR 
    DATE(date_voided) = '#{Current_date}') 
    AND person_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY person_id
  ").collect{|p|p["person_id"].to_i}

=begin
  Getting all changed rows in person_address table
=end    

  puts "Getting all changed rows in person_address table"

  person_address_patient_ids = Connection.select_all("
    SELECT person_id FROM person_address
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_voided) = '#{Current_date}') 
    AND person_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY person_id
  ").collect{|p|p["person_id"].to_i}

=begin
  Getting all changed rows in person_attribute table
=end    
  
   puts "Getting all changed rows in person_attribute table"

   person_attribute_patient_ids = Connection.select_all("
    SELECT person_id FROM person_attribute
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_changed) = '#{Current_date}' OR 
    DATE(date_voided) = '#{Current_date}') 
    AND person_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY person_id
  ").collect{|p|p["person_id"].to_i}

=begin
  Getting all changed rows in patient_identifier table
=end    

  puts "Getting all changed rows in patient_identifier table"

  patient_identifier_patient_ids = Connection.select_all("
    SELECT patient_id FROM patient_identifier
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_voided) = '#{Current_date}') 
    AND patient_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY patient_id
  ").collect{|p|p["patient_id"].to_i}

=begin
  Getting all changed rows in patient_program table
=end    

  puts "Getting all changed rows in patient_program table"

  patient_program_patient_ids = Connection.select_all("
    SELECT patient_id FROM patient_program
    WHERE (DATE(date_created) = '#{Current_date}' OR
    DATE(date_changed) = '#{Current_date}' OR 
    DATE(date_voided) = '#{Current_date}') 
    AND patient_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    GROUP BY patient_id
  ").collect{|p|p["patient_id"].to_i}

=begin
  Getting all changed rows in patient_state table
=end    

  puts "Getting all changed rows in patient_state table"

  patient_state_patient_ids = Connection.select_all("
    SELECT p.patient_id FROM patient_program p 
    inner join patient_state s ON s.patient_program_id = p.patient_program_id
    where (date(s.date_created)= '#{Current_date}' OR date(s.date_changed)='#{Current_date}')
    AND p.patient_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    group by p.patient_id
  ").collect{|p|p["p.patient_id"].to_i}


=begin
  Getting all changed rows in program_workflow table
=end    

  puts "Getting all changed rows in program_workflow table"

  program_workflow_patient_ids = Connection.select_all("
    SELECT p.patient_id FROM patient_program p 
    inner join program_workflow w ON p.program_id = w.program_id
    where (date(w.date_created)= '#{Current_date}' OR date(w.date_changed)='#{Current_date}')
    AND p.patient_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    group by p.patient_id
  ").collect{|p|p["p.patient_id"].to_i}


=begin
  Getting all changed rows in program_workflow_state table
=end    

  puts "Getting all changed rows in program_workflow_state table"

  program_workflow_state_patient_ids = Connection.select_all("
    SELECT p.patient_id from patient_program p
    inner join program_workflow w ON p.program_id = w.program_id
    inner join program_workflow_state s ON w.program_workflow_id = s.program_workflow_id
    where (date(s.date_created) = '#{Current_date}' OR date(s.date_changed) = '#{Current_date}')
    AND p.patient_id IN(#{patient_ids_in_earliest_start_date.join(',')})
    group by p.patient_id
  ").collect{|p|p["p.patient_id"].to_i}

=begin
  Getting all changed rows in relationship table
=end

  puts "Getting all changed rows in relationship table"

   relationship_patient_ids = Connection.select_all("
    SELECT person_a FROM relationship
    where (DATE(date_created) = '#{Current_date}' OR 
    DATE(date_voided) = '#{Current_date}')
    AND person_a IN(#{patient_ids_in_earliest_start_date.join(',')})
    group by person_a;
  ").collect{|p|p["patient_id"].to_i}


  patient_ids = (enc_patient_ids + obs_patient_ids + orders_patient_ids + patient_patient_ids + person_patient_ids + person_name_patient_ids + person_address_patient_ids + person_attribute_patient_ids + patient_identifier_patient_ids + patient_program_patient_ids + patient_state_patient_ids + program_workflow_patient_ids + program_workflow_state_patient_ids + relationship_patient_ids).uniq



  #1. Demographics lookup
  updating_person_table(person_patient_ids)

  #2. Demographics lookup (names)
  updating_person_name_table(person_name_patient_ids)

  #3. Demographics lookup (address)
  updating_person_address_table(person_address_patient_ids)

  #4. Demographics lookup (patient_identifier)
  updating_patient_identifier_table(patient_identifier_patient_ids)

  #5. Person attributes (person_attributes)
  updating_person_attributes_table(person_attribute_patient_ids)

  #6. Patient_program lookup (patient_program)
  updating_patient_program_table(patient_program_patient_ids)

  #7 Demographics lookup (relationship)
  upating_relationship_table(relationship_patient_ids)

  #8 upadating_other_fields()
  upadating_other_fields(patient_ids)

  #9 updating_orders_tables()
  updating_orders_tables(orders_patient_ids)

  #10 updating_drug_orders_table()
  updating_drug_orders_table(patient_ids)

  #11 Demographics lookup (obs)
  updating_obs_table(obs_patient_ids)

  #updating encounter table:
  updating_encounter_table(enc_patient_ids)

  #updating flat_cohort_table:
  updating_flat_cohort_table(patient_ids)


  puts "........... #{patient_ids.length}"
end

def updating_flat_cohort_table(patient_ids)
  data = get_earliest_start_date_patients_data(patient_ids)

  (data || []).each do |row|
  
    flat_table1_record = Connection.select_one("SELECT * FROM flat_table1 WHERE patient_id = #{row[:patient_id]}")     

    eligible    = flat_table1_record['earliest_start_date']
    
    gender                                      = flat_table1_record['gender']
    dob                                         = flat_table1_record['dob']
    age_in_days                                 = flat_table1_record['age_in_days']
    death_date                                  = flat_table1_record['death_date']
    reason_for_eligibility_record               = flat_table1_record['reason_for_eligibility']
    ever_registered_at_art_clinic               = flat_table1_record['ever_registered_at_art_clinic']
    date_art_last_taken                         = flat_table1_record['date_art_last_taken']
    pulmonary_tuberculosis_last_2_years         = flat_table1_record['pulmonary_tuberculosis_last_2_years']
    kaposis_sarcoma                             = flat_table1_record['kaposis_sarcoma']
    extrapulmonary_tuberculosis_v_date          = flat_table1_record['extrapulmonary_tuberculosis_v_date'].to_date rescue nil
    pulmonary_tuberculosis_v_date               = flat_table1_record['pulmonary_tuberculosis_v_date']
    pulmonary_tuberculosis_last_2_years_v_date  = flat_table1_record['pulmonary_tuberculosis_last_2_years_v_date']
    kaposis_sarcoma_v_date                      = flat_table1_record['kaposis_sarcoma_v_date']
    reason_for_starting_v_date                  = flat_table1_record['reason_for_starting_v_date']
    ever_registered_at_art_v_date               = flat_table1_record['ever_registered_at_art_v_date']
    date_art_last_taken_v_date                  = flat_table1_record['date_art_last_taken_v_date']
    taken_art_in_last_two_months_v_date         = flat_table1_record['taken_art_in_last_two_months_v_date']
    date_enrolled                               = flat_table1_record['date_enrolled']

    #puts ".......... extrapulmonary_tuberculosis_v_date  #{extrapulmonary_tuberculosis_v_date}"


    unless eligible.blank?

      record = Connection.select_one("SELECT id FROM flat_cohort_table WHERE patient_id = #{row[:patient_id]}")

      #puts "#{age_at_initiation_record}.....#{patient_id}"
      #puts "#{record} ........... "

      age_at_initiation = flat_table1_record['age_at_initiation']
      record_exists = record['id']

      if record_exists.blank?
        Connection.execute <<EOF
          INSERT INTO flat_cohort_table (patient_id, gender, birthdate, earliest_start_date, date_enrolled, age_at_initiation, age_in_days, death_date, reason_for_starting, ever_registered_at_art, date_art_last_taken, taken_art_in_last_two_months, extrapulmonary_tuberculosis, pulmonary_tuberculosis, pulmonary_tuberculosis_last_2_years, kaposis_sarcoma,extrapulmonary_tuberculosis_v_date, pulmonary_tuberculosis_v_date, pulmonary_tuberculosis_last_2_years_v_date, kaposis_sarcoma_v_date, reason_for_starting_v_date, ever_registered_at_art_v_date, date_art_last_taken_v_date, taken_art_in_last_two_months_v_date) VALUES ("#{row[:patient_id]}", "#{gender}", "#{dob}", "#{eligible}", "#{date_enrolled}", "#{age_at_initiation}", "#{age_in_days}", "#{death_date}", "#{reason_for_eligibility}", "#{ever_registered_at_art_clinic}", "#{date_art_last_taken}", "#{taken_art_in_last_two_months}", "#{extrapulmonary_tuberculosis}", "#{pulmonary_tuberculosis}", "#{pulmonary_tuberculosis_last_2_years}", "#{kaposis_sarcoma}", "#{extrapulmonary_tuberculosis_v_date}", "#{pulmonary_tuberculosis_v_date}", "#{pulmonary_tuberculosis_last_2_years_v_date}", "#{kaposis_sarcoma_v_date}", "#{reason_for_starting_v_date}", "#{ever_registered_at_art_v_date}", "#{date_art_last_taken_v_date}", "#{taken_art_in_last_two_months_v_date}");
EOF
        #puts "............ Inserting into flat_cohort_table (patient_id: #{row[:patient_id]})"

      else
        Connection.execute <<EOF
          UPDATE flat_cohort_table SET earliest_start_date = "#{eligible}", date_enrolled = "#{date_enrolled}", gender = "#{gender}", birthdate = "#{dob}", age_at_initiation = "#{age_at_initiation}", age_in_days = "#{age_in_days}" WHERE patient_id = "#{row[:patient_id]}";
EOF
          puts "........... Updating flat_cohort_table (patient_id: #{row[:patient_id]})"

          flat_cohort_record = Connection.select_one(" SELECT * FROM flat_cohort_table WHERE patient_id = #{row[:patient_id]} ")

          old_extrapulmonary_tuberculosis_v_date  = flat_cohort_record['extrapulmonary_tuberculosis_v_date'].to_date rescue nil
          old_pulmonary_tuberculosis_v_date       = flat_cohort_record['pulmonary_tuberculosis_v_date']
          old_pulmonary_tuberculosis_last_2_years_v_date  = flat_cohort_record['pulmonary_tuberculosis_last_2_years_v_date']
          old_kaposis_sarcoma_v_date                      = flat_cohort_record['kaposis_sarcoma_v_date']
          old_reason_for_starting_v_date                  = flat_cohort_record['reason_for_starting_v_date']
          old_ever_registered_at_art_v_date               = flat_cohort_record['ever_registered_at_art_v_date']
          old_date_art_last_taken_v_date                  = flat_cohort_record['date_art_last_taken_v_date']
          old_taken_art_in_last_two_months_v_date         = flat_cohort_record['taken_art_in_last_two_months_v_date']

          puts "....... old_extrapulmonary_tuberculosis_v_date   #{old_extrapulmonary_tuberculosis_v_date}"

          
      end #record_exists.blank

    end #unless eligible

  end #loop
end

def updating_obs_table(patient_ids)
  (patient_ids || []).each do |patient_id|

    flat_table2_record = Connection.select_one("SELECT * FROM flat_table2 WHERE patient_id = '#{patient_id}'
      AND (DATE(visit_date) = '#{Current_date}')")
    
    next if flat_table2_record.blank?

    visit =  flat_table2_record['ID']

    encounter_records = Connection.select_all("SELECT * FROM encounter WHERE patient_id = #{patient_id}
      AND (DATE(encounter_datetime) = '#{Current_date}')
      AND voided = 0
      AND encounter_type NOT IN (6, 7, 9, 25, 51, 52, 53, 54, 68, 119)")

    (encounter_records). each do |enc|
      encounter_type = enc['encounter_type']
      #puts "patient_id: #{patient_id} encounter_type: #{encounter_type}"

      obs_records = Connection.select_all("
      SELECT * FROM obs WHERE person_id = '#{patient_id}'
      AND (DATE(date_created) = '#{Current_date}' OR 
      DATE(date_voided) = '#{Current_date}')
    ")

      (obs_records).each do |ob|

        obs_datetime = ob['obs_datetime']
        concept_id = ob['concept_id']
        value_coded = ob['value_coded']
        value_coded_name_id = ob['value_coded_name_id']
        value_text = ob['value_text']
        value_numeric = ob['value_numeric']
        value_datetime = ob['value_datetime']
        value_modifier = ob['value_modifier']
        voided = ob['voided']
        encounter_id = ob['encounter_id']


      if encounter_type.present?

        pregnant_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
          LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
          WHERE name = 'Is patient pregnant?' AND voided = 0 AND retired = 0")
        pregnant = pregnant_record['concept_id']


        pregnant2_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'patient pregnant' AND voided = 0 AND retired = 0")
        pregnant2 = pregnant2_record['concept_id']


        breast_feeding_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Breastfeeding' AND voided = 0 AND retired = 0")
        breast_feeding = breast_feeding_record['concept_id']


        breast_feeding2_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Breast feeding?' AND voided = 0 AND retired = 0 ")
        breast_feeding2 = breast_feeding2_record['concept_id']


        currently_using_family_planning_method_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Currently using family planning method' AND voided = 0 AND retired = 0 ")
        currently_using_family_planning_method = currently_using_family_planning_method_record['concept_id']


        method_of_family_planning_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Method of family planning' AND voided = 0 AND retired = 0 ")
        method_of_family_planning = method_of_family_planning_record['concept_id']

        
        symptom_present_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Symptom present' AND voided = 0 AND retired = 0 LIMIT 1")
        symptom_present = symptom_present_record['concept_id']


        malawi_ART_side_effects_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'malawi ART side effects' AND voided = 0 AND retired = 0 ")
        malawi_ART_side_effects = malawi_ART_side_effects_record['concept_id']

        
        drug_induced_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Drug induced' AND voided = 0 AND retired = 0 ")
        drug_induced = drug_induced_record['concept_id']


        side_effects_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Malawi ART side effects' AND voided = 0 AND retired = 0")
        side_effects = side_effects_record['concept_id']

        
        routine_tb_screening_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Routine TB Screening' AND voided = 0 AND retired = 0")
        routine_tb_screening = routine_tb_screening_record['concept_id']

        
        allergic_to_sulphur_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Allergic to sulphur' AND voided = 0 AND retired = 0 ")
        allergic_to_sulphur = allergic_to_sulphur_record['concept_id']

    
        tb_status_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'TB status' AND voided = 0 AND retired = 0 ")
        tb_status = tb_status_record['concept_id']


        guardian_present_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Guardian Present' AND voided = 0 AND retired = 0 ")
        guardian_present = guardian_present_record['concept_id']


        patient_present_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Patient Present' AND voided = 0 AND retired = 0 LIMIT 1")
        patient_present = patient_present_record['concept_id']


        arv_regimen_type_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'What type of antiretroviral regimen' AND voided = 0 AND retired = 0 ")
        arv_regimen_type = arv_regimen_type_record['concept_id']


        cpt_given_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'CPT given' AND voided = 0 AND retired = 0 ")
        cpt_given = cpt_given_record['concept_id']  


        ipt_given_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Isoniazid' AND voided = 0 AND retired = 0 ")
        ipt_given = ipt_given_record['concept_id']


        prescribe_arvs_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Prescribe ARVs this visit' AND voided = 0 AND retired = 0 ")
        prescribe_arvs = prescribe_arvs_record['concept_id']


        continue_existing_regimen_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Continue existing regimen' AND voided = 0 AND retired = 0 ")
        continue_existing_regimen = continue_existing_regimen_record['concept_id']


        breastfeeding_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Breastfeeding' AND voided = 0 AND retired = 0 ")
        breastfeeding = breastfeeding_record['concept_id']


        transfer_within_responsibility_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Transfer within responsibility' AND voided = 0 AND retired = 0 ")
        transfer_within_responsibility = transfer_within_responsibility_record['concept_id']


  
        reason_for_eligibility_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                      LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                      WHERE name = 'Reason for ART eligibility' AND voided = 0 AND retired = 0 LIMIT 1")
        reason_for_eligibility = reason_for_eligibility['concept_id'] rescue nil

        
        who_stage_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                      LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                      WHERE name = 'WHO stage' AND voided = 0 AND retired = 0 ")
        who_stage = who_stage_record['concept_id']


        send_sms_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                      LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                      WHERE name = 'send sms' AND voided = 0 AND retired = 0 ")
        send_sms = send_sms_record['concept_id']


        agrees_to_followup_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                      LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                      WHERE name = 'Agrees to followup' AND voided = 0 AND retired = 0 ")
        agrees_to_followup = agrees_to_followup_record['concept_id']


        type_of_confirmatory_hiv_test_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                      LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                      WHERE name = 'Confirmatory HIV test type' AND voided = 0 AND retired = 0 ")
        type_of_confirmatory_hiv_test = type_of_confirmatory_hiv_test_record['concept_id']


        confirmatory_hiv_test_location_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                      LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                      WHERE name = 'confirmatory hiv test location' AND voided = 0 AND retired = 0 ")
        confirmatory_hiv_test_location = confirmatory_hiv_test_location_record['concept_id']


        cd4_count_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
          LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
          WHERE name = 'Cd4 count' AND voided = 0 AND retired = 0 ")
        cd4_count = cd4_count_record['concept_id']


        confirmatory_hiv_test_date_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
                      LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                      WHERE name = 'Confirmatory HIV test date' AND voided = 0 AND retired = 0 ")
        confirmatory_hiv_test_date = confirmatory_hiv_test_date_record['concept_id']

        #raise patient_present.inspect
        case concept_id
        when patient_present
          yes_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'Yes' AND voided = 0 AND retired = 0 ")
          yes = yes_record['concept_id']

          no_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'No' AND voided = 0 AND retired = 0 ")
          no = no_record['concept_id']

          unknown_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'Unknown' AND voided = 0 AND retired = 0 ")
          unknown = unknown_record['concept_id']

          case value_coded
          when yes

            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if value.blank?
              if (value_text == 'Yes')
                value = 'Yes'
              end #value_text
            end #value

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, patient_present_yes, patient_present_yes_enc_id) VALUES ("#{patient_id}", "#{Current_date}", "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (patient_present: value_coded = yes): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET patient_present_yes = "#{value}", patient_present_no = NULL, patient_present_yes_enc_id = "#{encounter_id}", patient_present_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (patient_present: value_coded = yes): #{patient_id}"
              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET patient_present_yes = NULL, patient_present_no = NULL, patient_present_yes_enc_id = NULL, patient_present_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (patient_present: value_coded = yes): #{patient_id}"
            end #voided

          
          when no
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if value.blank?
              if value_text.blank?
                value = 'No'
              end #value_text
            end #value

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, patient_present_no, patient_present_no_enc_id) VALUES ("#{patient_id}", "#{Current_date}", "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (patient_present: value_coded = no): #{patient_id}"
              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET patient_present_no = "#{value}", patient_present_yes = NULL, patient_present_no_enc_id = "#{encounter_id}", patient_present_yes_enc_id = NULL WHERE flat_table2.id = "#{visit_id}";
EOF
                puts "........... Updating record into flat_table2 (patient_present: value_coded = no): #{patient_id}"
              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET patient_present_no = NULL, patient_present_yes = NULL, patient_present_no_enc_id = NULL, patient_present_yes_enc_id = NULL WHERE flat_table2.id = "#{visit_id}";
EOF
              puts "........... Updating record into flat_table2 (patient_present: value_coded = no): #{patient_id}"
            end #voided
          

          when unknown
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, patient_present_unknown, patient_present_unknown_enc_id) VALUES ("#{patient_id}", "#{Current_date}", "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (patient_present: value_coded = unknown): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET patient_present_no = NULL, patient_present_yes = NULL, patient_present_no_enc_id = NULL, patient_present_yes_enc_id = NULL, patient_present_unknown = @"#{value}", patient_present_unknown_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (patient_present: value_coded = unknown): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET patient_present_no = NULL, patient_present_yes = NULL, patient_present_no_enc_id = NULL, patient_present_yes_enc_id = NULL, patient_present_unknown = NULL, patient_present_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (patient_present: value_coded = unknown): #{patient_id}"

            end #voided
    
          end #case

          if (value_text == 'Yes')
            patient_yes = 'Yes'

          elsif (value_text == 'YES')
            patient_yes = 'Yes'

          elsif (value_text == 'No')
            patient_no = 'No'

          elsif (value_text == 'NO')
            patient_no = 'No'

          elsif (value_text == 'Unknown')
            patient_unknown = 'Uknown'

          elsif (value_text == 'UNKNOWN')
            patient_unknown = 'Uknown'
          end #value_text

          case value_text
          when patient_yes
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, patient_present_yes, patient_present_yes_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{patient_yes}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (patient_present: value_text = patient_yes): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET patient_present_yes = "#{patient_yes}", patient_present_no = NULL, patient_present_yes_enc_id = "#{encounter_id}", patient_present_no_enc_id = NULL, patient_present_unknown = NULL, patient_present_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF                
                puts "........... Updating record into flat_table2 (patient_present: value_text = patient_yes): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET patient_present_yes = NULL, patient_present_no = NULL, patient_present_yes_enc_id = NULL, patient_present_no_enc_id = NULL, patient_present_unknown = NULL, patient_present_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (patient_present: value_text = patient_yes): #{patient_id}"

            end #voided
       
          end
          when patient_no
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, patient_present_no, patient_present_no_enc_id) VALUES ("#{patient_id}", "#{Current_date}", "#{patient_no}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (patient_present: value_text = patient_no): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET patient_present_no = "#{patient_no}", patient_present_yes = NULL, patient_present_no_enc_id = "#{encounter_id}", patient_present_yes_enc_id = NULL, patient_present_unknown = NULL, patient_present_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";    
EOF
                puts "........... Updating record into flat_table2 (patient_present: value_text = patient_no): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET patient_present_no = NULL, patient_present_yes = NULL, patient_present_no_enc_id = NULL, patient_present_yes_enc_id = NULL, patient_present_unknown = NULL, patient_present_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (patient_present: value_text = patient_no): #{patient_id}"

            end #voided

          when patient_unknown
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, patient_present_unknown, patient_present_unknown_enc_id) VALUES ("#{patient_id}", "#{Current_date}", "#{patient_unknown}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (patient_present: value_text = patient_unknown): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET patient_present_no = NULL, patient_present_yes = NULL, patient_present_no_enc_id = NULL, patient_present_yes_enc_id = NULL, patient_present_unknown = "#{patient_unknown}", patient_present_unknown_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}"; 
EOF
                puts "........... Updating record into flat_table2 (patient_present: value_text = patient_unknown): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET patient_present_no = NULL, patient_present_yes = NULL, patient_present_no_enc_id = NULL, patient_present_yes_enc_id = NULL, patient_present_unknown = NULL, patient_present_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}"; 
EOF
              puts "........... Updating record into flat_table2 (patient_present: value_text = patient_unknown): #{patient_id}"

            end #voided

          end #case

        when guardian_present
            yes_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'Yes' AND voided = 0 AND retired = 0 ")
          yes = yes_record['concept_id']

          no_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'No' AND voided = 0 AND retired = 0 ")
          no = no_record['concept_id']

          unknown_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'Unknown' AND voided = 0 AND retired = 0 ")
          unknown = unknown_record['concept_id']

          case value_coded
          when yes
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id} ")
            value = value_record['name']

            if value.blank?
              if (value_text == 'Yes')
                value = 'Yes'

              elsif (value_text == 'YES')
                value = 'Yes'
              end
            end

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, guardian_present_yes, guardian_present_yes_enc_id) VALUES ("#{patient_id}", "#{Current_date}", "#{value}", "#{encounter_id}");
EOF
              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET guardian_present_yes = "#{value}", guardian_present_no = NULL, guardian_present_yes_enc_id = "#{encounter_id}", guardian_present_no_enc_id = NULL, guardian_present_unknown = NULL, guardian_present_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              end

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET guardian_present_yes = NULL, guardian_present_no = NULL, guardian_present_yes_enc_id = NULL, guardian_present_no_enc_id = NULL, guardian_present_unknown = NULL, guardian_present_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
            end

          when no
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if value.blank?
              if (value_text == 'No')
                value = 'No'

              elsif (value_text == 'NO')
                value_text = 'No'
              end
            end #end value

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, guardian_present_no, guardian_present_no_enc_id) VALUES ("#{patient_id}", "#{Current_date}", "#{value}", "#{encounter_id}");
EOF
              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET guardian_present_no = "#{value}", guardian_present_yes = NULL, guardian_present_no_enc_id = encounter_id, guardian_present_yes_enc_id = NULL, guardian_present_unknown = NULL, guardian_present_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              end #end visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET guardian_present_no = NULL, guardian_present_yes = NULL, guardian_present_no_enc_id = NULL, guardian_present_yes_enc_id = NULL, guardian_present_unknown = NULL, guardian_present_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
            end #end voided

          when unknown
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if value.blank?
              if (value_text == 'Unknown')
                value = 'Unknown'
              end
            end #end value

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, guardian_present_unknown, guardian_present_unknown_enc_id) VALUES ("#{patient_id}", "#{visit}", "#{value}", "#{encounter_id}");
EOF
              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET guardian_present_no = NULL, guardian_present_yes = NULL, guardian_present_no_enc_id = NULL, guardian_present_yes_enc_id = NULL, guardian_present_unknown = "#{value}", guardian_present_unknown_enc_id = encounter_id WHERE flat_table2.id = "#{visit}";
EOF                
              end #end visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET guardian_present_no = NULL, guardian_present_yes = NULL, guardian_present_no_enc_id = NULL, guardian_present_yes_enc_id = NULL, guardian_present_unknown = NULL, guardian_present_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
            end #end voided
          end


          end       
            
          if (value_text == 'YES')
            guardian_yes = 'Yes'

          elsif (value_text == 'Yes')
            guardian_yes = 'Yes'

          elsif (value_text == 'NO')
            guardian_no = 'No'

          elsif (value_text == 'No')
            guardian_no = 'No'

          elsif (value_text == 'Unknown')
            guardian_unknown = 'Unknown'

          elsif (value_text == 'UNKNOWN')
            guardian_unknown = 'Unknown'
          end

          case value_text
          when guardian_yes
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, guardian_present_yes, guardian_present_yes_enc_id) VALUES ("#{patient_id}", "#{Current_date}", "#{guardian_yes}", "#{encounter_id}");
EOF
              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET guardian_present_yes = "#{guardian_yes}", guardian_present_no = NULL, guardian_present_yes_enc_id = "#{encounter_id}", guardian_present_no_enc_id = NULL, guardian_present_unknown = NULL, guardian_present_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";

              end #end visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET guardian_present_yes = NULL, guardian_present_no = NULL, guardian_present_yes_enc_id = NULL, guardian_present_no_enc_id = NULL, guardian_present_unknown = NULL, guardian_present_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
            end #end voided
          end
          when guardian_no
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, guardian_present_no, guardian_present_no_enc_id) VALUES ("#{patient_id}", "#{Current_date}", "#{guardian_no}", "#{encounter_id}");
EOF
              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET guardian_present_no = "#{guardian_no}", guardian_present_yes = NULL, guardian_present_no_enc_id = "#{encounter_id}", guardian_present_yes_enc_id = NULL, guardian_present_unknown = NULL, guardian_present_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";    
EOF
              end #end visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET guardian_present_no = NULL, guardian_present_yes = NULL, guardian_present_no_enc_id = NULL, guardian_present_yes_enc_id = NULL, guardian_present_unknown = NULL, guardian_present_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
            end #end voided

          when guardian_unknown
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, guardian_present_unknown, guardian_present_unknown_enc_id) VALUES ("#{patient_id}", "#{Current_date}", "#{guardian_unknown}", encounter_id);
EOF
              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET guardian_present_no = NULL, guardian_present_yes = NULL, guardian_present_no_enc_id = NULL, guardian_present_yes_enc_id = NULL, guardian_present_unknown = "#{guardian_unknown}", guardian_present_unknown_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}"; 
EOF
              end #end visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET guardian_present_no = NULL, guardian_present_yes = NULL, guardian_present_no_enc_id = NULL, guardian_present_yes_enc_id = NULL, guardian_present_unknown = NULL, guardian_present_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}"; 
EOF
            end #end voided
          end # end case

        when transfer_within_responsibility
          yes_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'Yes' AND voided = 0 AND retired = 0 ")
          yes = yes_record['concept_id']

          no_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'No' AND voided = 0 AND retired = 0 ")
          no = no_record['concept_id']

          case value_coded
          when yes
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = in_field_value_coded_name_id")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, transfer_within_responsibility_yes, transfer_within_responsibility_yes_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET transfer_within_responsibility_yes = "#{value}", transfer_within_responsibility_no = NULL, transfer_within_responsibility_yes_enc_id = "#{encounter_id}", transfer_within_responsibility_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET transfer_within_responsibility_yes = NULL, transfer_within_responsibility_no = NULL, transfer_within_responsibility_yes_enc_id = NULL, transfer_within_responsibility_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
            end #voided

          when no
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = in_field_value_coded_name_id")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, transfer_within_responsibility_no, transfer_within_responsibility_no_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET transfer_within_responsibility_no = "#{value}", transfer_within_responsibility_yes = NULL, transfer_within_responsibility_no_enc_id = "#{encounter_id}", transfer_within_responsibility_yes_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET transfer_within_responsibility_no = NULL, transfer_within_responsibility_yes = NULL, transfer_within_responsibility_no_enc_id = NULL, transfer_within_responsibility_yes_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
            end #voided
          end #case value_coded
          
        when breastfeeding
          yes_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'Yes' AND voided = 0 AND retired = 0 ")
          yes = yes_record['concept_id']

          no_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'No' AND voided = 0 AND retired = 0 ")
          no = no_record['concept_id']

          unknown_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'Unknown' AND voided = 0 AND retired = 0 ")
          unknown = unknown_record['concept_id']

          case value_coded
          when yes
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            encounter_type_record = Connection.select_one("SELECT * FROM encounter e WHERE e.encounter_id = #{encounter_id} AND voided = #{voided}")
            encounter_type = encounter_type_record['encounter_type']

            if visit.blank?
              if (encounter_type == 52)
                Connection.execute <<EOF
                INSERT INTO flat_table1 (patient_id, breastfeeding_yes, breastfeeding_yes_enc_id, breastfeeding_yes_v_date) VALUES ("#{patient_id}", "#{value}", "#{encounter_id}", "#{Current_date}");
EOF
              elsif (encounter_type == 53)
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, breastfeeding_yes, breastfeeding_yes_enc_id) VALUES ("#{patient_id}", "#{Current_date}", "#{value}", "#{encounter_id}");
EOF
              end #encounter type

            else
              if (encounter_type == 52)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_yes = "#{value}", breastfeeding_no = NULL, breastfeeding_unknown = NULL, breastfeeding_yes_enc_id = "#{encounter_id}", breastfeeding_no_enc_id = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_yes_v_date = "#{Current_date}", breastfeeding_no_v_date = NULL, breastfeeding_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_yes_v_date = NULL, breastfeeding_no_v_date = NULL, breastfeeding_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                end #voided

              elsif (encounter_type == 53)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_yes = @value, breastfeeding_no = NULL, breastfeeding_unknown = NULL, breastfeeding_yes_enc_id = "#{encounter_id}", breastfeeding_no_enc_id = NULL, breastfeeding_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                end #voided
              end #encounter_type
            end #visit

          when no
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            encounter_type_record = Connection.select_one("SELECT * FROM encounter e WHERE e.encounter_id = #{encounter_id} AND voided = #{voided}")
            encounter_type = encounter_type_record['encounter_type']  

            if visit.blank?
              if (encounter_type == 52)
                Connection.execute <<EOF
                INSERT INTO flat_table1 (patient_id, breastfeeding_no, breastfeeding_no_enc_id, breastfeeding_no_v_date) VALUES ("#{patient_id}", "#{value}", "#{encounter_id}", "#{Current_date}");
EOF
              elsif (encounter_type == 53)
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, breastfeeding_no, breastfeeding_no_enc_id) VALUES ("#{patient_id}", in_visit_date, @value, encounter_id);
EOF
              end #encounter_type

              if (encounter_type == 52)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_no = "#{value}", breastfeeding_yes = NULL, breastfeeding_unknown = NULL, breastfeeding_no_enc_id = "#{encounter_id}", breastfeeding_yes_enc_id = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_no_v_date = "#{Current_date}", breastfeeding_yes_v_date = NULL, breastfeeding_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_no = NULL, breastfeeding_yes = NULL, breastfeeding_unknown = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_no_v_date = NULL, breastfeeding_yes_v_date = NULL, breastfeeding_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                end #voided
              end #encounter type==52

              if (encounter_type == 53)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_no = "#{value}", breastfeeding_yes = NULL, breastfeeding_unknown = NULL, breastfeeding_no_enc_id = "#{encounter_id}", breastfeeding_yes_enc_id = NULL, breastfeeding_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_no = NULL, breastfeeding_yes = NULL, breastfeeding_unknown = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                end #voided
              end #encounter type==53
            end #visit

          when unknown
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            encounter_type_record = Connection.select_one("SELECT * FROM encounter e WHERE e.encounter_id = #{encounter_id} AND voided = #{voided}")
            encounter_type = encounter_type_record['encounter_type']

            if visit.blank?
              if (encounter_type == 52)
                Connection.execute <<EOF
                INSERT INTO flat_table1 (patient_id, breastfeeding_unknown, breastfeeding_unknown_enc_id, breastfeeding_unknown_v_date) VALUES ("#{patient_id}", "#{value}", "#{encounter_id}", "#{Current_date}");
EOF
              elsif (encounter_type == 53)
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, breastfeeding_unknown, breastfeeding_unknown_enc_id) VALUES (in_patient_id, "#{Current_date}", "#{value}", "#{encounter_id}");
EOF
              end #encounter type

              if (encounter_type == 52)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_unknown = "#{value}", breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown_enc_id = "#{encounter_id}", breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_unknown_v_date = "#{Current_date}", breastfeeding_yes_v_date = NULL, breastfeeding_no_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_unknown = NULL, breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_unknown_v_date = NULL, breastfeeding_yes_v_date = NULL, breastfeeding_no_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                end #voided
              end #encounter_type==52

              if (encounter_type == 53)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_unknown = "#{value}", breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown_enc_id = "#{encounter_id}", breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_unknown = NULL, breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                end #voided

              end #encounter_type == 53

            end #visit

          end #end case

        when breast_feeding
          yes_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'Yes' AND voided = 0 AND retired = 0 ")
          yes = yes_record['concept_id']

          no_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'No' AND voided = 0 AND retired = 0 ")
          no = no_record['concept_id']

          unknown_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'Unknown' AND voided = 0 AND retired = 0 ")
          unknown = unknown_record['concept_id']

          case value_coded
          when yes
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            encounter_type_record = Connection.select_one("SELECT * FROM encounter e WHERE e.encounter_id = #{encounter_id} AND voided = #{voided}")
            encounter_type = encounter_type_record['encounter_type']

            if visit.blank?
              if (encounter_type == 52)
                Connection.execute <<EOF
                INSERT INTO flat_table1 (patient_id, breastfeeding_yes, breastfeeding_yes_enc_id, breastfeeding_yes_v_date) VALUES ("#{patient_id}", "#{value}", "#{encounter_id}", '#{Current_date}');
EOF
              elsif (encounter_type == 53)
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, breastfeeding_yes, breastfeeding_yes_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
              end #encounter type

            else
              if (encounter_type == 52)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_yes = "#{value}", breastfeeding_no = NULL, breastfeeding_unknown = NULL, breastfeeding_yes_enc_id = "#{encounter_id}", breastfeeding_no_enc_id = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_yes_v_date = '#{Current_date}', breastfeeding_no_v_date = NULL, breastfeeding_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_yes_v_date = NULL, breastfeeding_no_v_date = NULL, breastfeeding_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                end #voided

              elsif (encounter_type == 53)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_yes = @value, breastfeeding_no = NULL, breastfeeding_unknown = NULL, breastfeeding_yes_enc_id = "#{encounter_id}", breastfeeding_no_enc_id = NULL, breastfeeding_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                end #voided
              end #encounter_type
            end #visit

          when no
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            encounter_type_record = Connection.select_one("SELECT * FROM encounter e WHERE e.encounter_id = #{encounter_id} AND voided = #{voided}")
            encounter_type = encounter_type_record['encounter_type']  

            if visit.blank?
              if (encounter_type == 52)
                Connection.execute <<EOF
                INSERT INTO flat_table1 (patient_id, breastfeeding_no, breastfeeding_no_enc_id, breastfeeding_no_v_date) VALUES ("#{patient_id}", "#{value}", "#{encounter_id}", "#{Current_date}");
EOF
              elsif (encounter_type == 53)
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, breastfeeding_no, breastfeeding_no_enc_id) VALUES ("#{patient_id}", in_visit_date, @value, encounter_id);
EOF
              end #encounter_type

              if (encounter_type == 52)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_no = "#{value}", breastfeeding_yes = NULL, breastfeeding_unknown = NULL, breastfeeding_no_enc_id = "#{encounter_id}", breastfeeding_yes_enc_id = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_no_v_date = "#{Current_date}", breastfeeding_yes_v_date = NULL, breastfeeding_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_no = NULL, breastfeeding_yes = NULL, breastfeeding_unknown = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_no_v_date = NULL, breastfeeding_yes_v_date = NULL, breastfeeding_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                end #voided
              end #encounter type==52

              if (encounter_type == 53)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_no = "#{value}", breastfeeding_yes = NULL, breastfeeding_unknown = NULL, breastfeeding_no_enc_id = "#{encounter_id}", breastfeeding_yes_enc_id = NULL, breastfeeding_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_no = NULL, breastfeeding_yes = NULL, breastfeeding_unknown = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                end #voided
              end #encounter type==53
            end #visit

          when unknown
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            encounter_type_record = Connection.select_one("SELECT * FROM encounter e WHERE e.encounter_id = #{encounter_id} AND voided = #{voided}")
            encounter_type = encounter_type_record['encounter_type']

            if visit.blank?
              if (encounter_type == 52)
                Connection.execute <<EOF
                INSERT INTO flat_table1 (patient_id, breastfeeding_unknown, breastfeeding_unknown_enc_id, breastfeeding_unknown_v_date) VALUES ("#{patient_id}", "#{value}", "#{encounter_id}", "#{Current_date}");
EOF
              elsif (encounter_type == 53)
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, breastfeeding_unknown, breastfeeding_unknown_enc_id) VALUES (in_patient_id, "#{Current_date}", "#{value}", "#{encounter_id}");
EOF
              end #encounter type

              if (encounter_type == 52)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_unknown = "#{value}", breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown_enc_id = "#{encounter_id}", breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_unknown_v_date = "#{Current_date}", breastfeeding_yes_v_date = NULL, breastfeeding_no_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_unknown = NULL, breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_unknown_v_date = NULL, breastfeeding_yes_v_date = NULL, breastfeeding_no_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                end #voided
              end #encounter_type==52

              if (encounter_type == 53)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_unknown = "#{value}", breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown_enc_id = "#{encounter_id}", breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_unknown = NULL, breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                end #voided

              end #encounter_type == 53

            end #visit

          end #end case

        when breast_feeding2
          yes_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'Yes' AND voided = 0 AND retired = 0 ")
          yes = yes_record['concept_id']

          no_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'No' AND voided = 0 AND retired = 0 ")
          no = no_record['concept_id']

          unknown_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'Unknown' AND voided = 0 AND retired = 0 ")
          unknown = unknown_record['concept_id']

          case value_coded
          when yes
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            encounter_type_record = Connection.select_one("SELECT * FROM encounter e WHERE e.encounter_id = #{encounter_id} AND voided = #{voided}")
            encounter_type = encounter_type_record['encounter_type']

            if visit.blank?
              if (encounter_type == 52)
                Connection.execute <<EOF
                INSERT INTO flat_table1 (patient_id, breastfeeding_yes, breastfeeding_yes_enc_id, breastfeeding_yes_v_date) VALUES ("#{patient_id}", "#{value}", "#{encounter_id}", "#{Current_date}");
EOF
              elsif (encounter_type == 53)
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, breastfeeding_yes, breastfeeding_yes_enc_id) VALUES ("#{patient_id}", "#{Current_date}", "#{value}", "#{encounter_id}");
EOF
              end #encounter type

            else
              if (encounter_type == 52)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_yes = "#{value}", breastfeeding_no = NULL, breastfeeding_unknown = NULL, breastfeeding_yes_enc_id = "#{encounter_id}", breastfeeding_no_enc_id = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_yes_v_date = "#{Current_date}", breastfeeding_no_v_date = NULL, breastfeeding_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_yes_v_date = NULL, breastfeeding_no_v_date = NULL, breastfeeding_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                end #voided

              elsif (encounter_type == 53)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_yes = @value, breastfeeding_no = NULL, breastfeeding_unknown = NULL, breastfeeding_yes_enc_id = "#{encounter_id}", breastfeeding_no_enc_id = NULL, breastfeeding_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                end #voided
              end #encounter_type
            end #visit

          when no
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            encounter_type_record = Connection.select_one("SELECT * FROM encounter e WHERE e.encounter_id = #{encounter_id} AND voided = #{voided}")
            encounter_type = encounter_type_record['encounter_type']  

            if visit.blank?
              if (encounter_type == 52)
                Connection.execute <<EOF
                INSERT INTO flat_table1 (patient_id, breastfeeding_no, breastfeeding_no_enc_id, breastfeeding_no_v_date) VALUES ("#{patient_id}", "#{value}", "#{encounter_id}", "#{Current_date}");
EOF
              elsif (encounter_type == 53)
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, breastfeeding_no, breastfeeding_no_enc_id) VALUES ("#{patient_id}", in_visit_date, @value, encounter_id);
EOF
              end #encounter_type

              if (encounter_type == 52)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_no = "#{value}", breastfeeding_yes = NULL, breastfeeding_unknown = NULL, breastfeeding_no_enc_id = "#{encounter_id}", breastfeeding_yes_enc_id = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_no_v_date = "#{Current_date}", breastfeeding_yes_v_date = NULL, breastfeeding_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_no = NULL, breastfeeding_yes = NULL, breastfeeding_unknown = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_no_v_date = NULL, breastfeeding_yes_v_date = NULL, breastfeeding_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                end #voided
              end #encounter type==52

              if (encounter_type == 53)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_no = "#{value}", breastfeeding_yes = NULL, breastfeeding_unknown = NULL, breastfeeding_no_enc_id = "#{encounter_id}", breastfeeding_yes_enc_id = NULL, breastfeeding_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_no = NULL, breastfeeding_yes = NULL, breastfeeding_unknown = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                end #voided
              end #encounter type==53
            end #visit

          when unknown
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            encounter_type_record = Connection.select_one("SELECT * FROM encounter e WHERE e.encounter_id = #{encounter_id} AND voided = #{voided}")
            encounter_type = encounter_type_record['encounter_type']

            if visit.blank?
              if (encounter_type == 52)
                Connection.execute <<EOF
                INSERT INTO flat_table1 (patient_id, breastfeeding_unknown, breastfeeding_unknown_enc_id, breastfeeding_unknown_v_date) VALUES ("#{patient_id}", "#{value}", "#{encounter_id}", "#{Current_date}");
EOF
              elsif (encounter_type == 53)
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, breastfeeding_unknown, breastfeeding_unknown_enc_id) VALUES (in_patient_id, "#{Current_date}", "#{value}", "#{encounter_id}");
EOF
              end #encounter type

              if (encounter_type == 52)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_unknown = "#{value}", breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown_enc_id = "#{encounter_id}", breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_unknown_v_date = "#{Current_date}", breastfeeding_yes_v_date = NULL, breastfeeding_no_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET breastfeeding_unknown = NULL, breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL, breastfeeding_unknown_v_date = NULL, breastfeeding_yes_v_date = NULL, breastfeeding_no_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                end #voided
              end #encounter_type==52

              if (encounter_type == 53)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_unknown = "#{value}", breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown_enc_id = "#{encounter_id}", breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET breastfeeding_unknown = NULL, breastfeeding_yes = NULL, breastfeeding_no = NULL, breastfeeding_unknown_enc_id = NULL, breastfeeding_yes_enc_id = NULL, breastfeeding_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                end #voided

              end #encounter_type == 53

            end #visit

          end #end case

        when continue_existing_regimen
          yes_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'Yes' AND voided = 0 AND retired = 0 ")
          yes = yes_record['concept_id']

          no_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'No' AND voided = 0 AND retired = 0 ")
          no = no_record['concept_id']

          case value_coded
          when yes

            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']
            
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, continue_existing_regimen_yes, continue_existing_regimen_yes_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (continue_existing_regimen: value_coded = yes): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET continue_existing_regimen_yes = "#{value}", continue_existing_regimen_no = NULL, continue_existing_regimen_yes_enc_id = "#{encounter_id}", continue_existing_regimen_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (continue_existing_regimen: value_coded = yes): #{patient_id}"

              end #visit

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET continue_existing_regimen_yes = NULL, continue_existing_regimen_no = NULL, continue_existing_regimen_yes_enc_id = NULL, continue_existing_regimen_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (continue_existing_regimen: value_coded = yes): #{patient_id}"
            end #voided

          when no

            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, continue_existing_regimen_no, continue_existing_regimen_no_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (continue_existing_regimen: value_coded = no): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET continue_existing_regimen_no = "#{value}", continue_existing_regimen_yes = NULL, continue_existing_regimen_yes_enc_id = NULL, continue_existing_regimen_no_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (continue_existing_regimen: value_coded = no): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET continue_existing_regimen_no = NULL, continue_existing_regimen_yes = NULL, continue_existing_regimen_yes_enc_id = NULL, continue_existing_regimen_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
            end #voided
          end #case

        when prescribe_arvs
          yes_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'Yes' AND voided = 0 AND retired = 0 ")
          yes = yes_record['concept_id']

          no_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'No' AND voided = 0 AND retired = 0 ")
          no = no_record['concept_id']

          case value_coded
          when yes
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, prescribe_arvs_yes, prescribe_arvs_yes_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (prescribe_arvs: value_coded==yes): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET prescribe_arvs_yes = "#{value}", prescribe_arvs_no = NULL, prescribe_arvs_yes_enc_id = "#{encounter_id}", prescribe_arvs_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (prescribe_arvs: value_coded==yes): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET prescribe_arvs_yes = NULL, prescribe_arvs_no = NULL, prescribe_arvs_yes_enc_id = NULL, prescribe_arvs_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (prescribe_arvs: value_coded==yes): #{patient_id}"

            end #voided

          when no
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, prescribe_arvs_no, prescribe_arvs_no_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (prescribe_arvs: value_coded==no): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET prescribe_arvs_no = @value, prescribe_arvs_yes = NULL, prescribe_arvs_no_enc_id = "#{encounter_id}", prescribe_arvs_yes_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (prescribe_arvs: value_coded==no): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET prescribe_arvs_no = NULL, prescribe_arvs_yes = NULL, prescribe_arvs_no_enc_id = NULL, prescribe_arvs_yes_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (prescribe_arvs: value_coded==no): #{patient_id}"

            end #voided
          end #case


        when cpt_given
          yes_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'Yes' AND voided = 0 AND retired = 0 ")
          yes = yes_record['concept_id']

          no_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'No' AND voided = 0 AND retired = 0 ")
          no = no_record['concept_id']

          case value_coded
          when yes
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if value.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, cpt_given_yes, cpt_given_yes_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (cpt_given: value_coded==yes): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET cpt_given_yes = "#{value}", cpt_given_no = NULL, cpt_given_yes_enc_id = "#{encounter_id}", cpt_given_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (cpt_given: value_coded==yes): #{patient_id}"

              end #visit
            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET cpt_given_yes = NULL, cpt_given_no = NULL, cpt_given_yes_enc_id = NULL, cpt_given_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Inserting record into flat_table2 (cpt_given: value_coded==yes): #{patient_id}"

            end #value

          when no
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if value.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, cpt_given_no, cpt_given_no_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (cpt_given: value_coded==no): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET cpt_given_no = "#{value}", cpt_given_yes = NULL, cpt_given_no_enc_id = "#{encounter_id}", cpt_given_yes_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (cpt_given: value_coded==no): #{patient_id}"

              end #visit
            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET cpt_given_no = NULL, cpt_given_yes = NULL, cpt_given_no_enc_id = NULL, cpt_given_yes_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (cpt_given: value_coded==no): #{patient_id}"

            end #value
          end #case


        when arv_regimen_type
          arv_regimen_type_unknown_record = Connection.select_one("SELECT concept_name.concept_name_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'UNKNOWN ANTIRETROVIRAL DRUG' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC")
          arv_regimen_type_unknown = arv_regimen_type_unknown_record['concept_name_id']
    
          arv_regimen_type_d4T_3TC_NVP_record = Connection.select_one("SELECT concept_name.concept_name_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'd4T/3TC/NVP' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          arv_regimen_type_d4T_3TC_NVP = arv_regimen_type_d4T_3TC_NVP_record['concept_name_id']
    
          arv_regimen_type_triomune_record = Connection.select_one("SELECT concept_name.concept_name_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'triomune' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          arv_regimen_type_triomune = arv_regimen_type_triomune_record['concept_name_id']
    
          arv_regimen_type_triomune_30_record = Connection.select_one("SELECT concept_name.concept_name_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'triomune-30' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          arv_regimen_type_triomune_30 = arv_regimen_type_triomune_30_record['concept_name_id']
    
          arv_regimen_type_triomune_40_record = Connection.select_one("SELECT concept_name.concept_name_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'triomune-40' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          arv_regimen_type_triomune_40 = arv_regimen_type_triomune_40_record['concept_name_id']
    
          arv_regimen_type_AZT_3TC_NVP_record = Connection.select_one("SELECT concept_name.concept_name_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'AZT/3TC/NVP' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          arv_regimen_type_AZT_3TC_NVP = arv_regimen_type_AZT_3TC_NVP_record['concept_name_id']
    
          arv_regimen_type_AZT_3TC_LPV_r_record = Connection.select_one("SELECT concept_name.concept_name_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'AZT/3TC+LPV/r' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          arv_regimen_type_AZT_3TC_LPV_r = arv_regimen_type_AZT_3TC_LPV_r_record['concept_name_id']
    
          arv_regimen_type_AZT_3TC_EFV_record = Connection.select_one("SELECT concept_name.concept_name_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'AZT/3TC+EFV' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          arv_regimen_type_AZT_3TC_EFV = arv_regimen_type_AZT_3TC_EFV_record['concept_name_id']
    
          arv_regimen_type_d4T_3TC_EFV_record = Connection.select_one("SELECT concept_name.concept_name_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'd4T/3TC/EFV' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          arv_regimen_type_d4T_3TC_EFV = arv_regimen_type_d4T_3TC_EFV_record['concept_name_id']
    
          arv_regimen_type_TDF_3TC_NVP_record = Connection.select_one("SELECT concept_name.concept_name_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'TDF/3TC+NVP' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          arv_regimen_type_TDF_3TC_NVP = arv_regimen_type_TDF_3TC_NVP_record['concept_name_id']
    
          arv_regimen_type_TDF_3TC_EFV_record = Connection.select_one("SELECT concept_name.concept_name_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'TDF/3TC/EFV' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          arv_regimen_type_TDF_3TC_EFV = arv_regimen_type_TDF_3TC_EFV_record['concept_name_id']
    
          arv_regimen_type_ABC_3TC_LPV_r_record = Connection.select_one("SELECT concept_name.concept_name_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'ABC/3TC+LPV/r' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          arv_regimen_type_ABC_3TC_LPV_r = arv_regimen_type_ABC_3TC_LPV_r_record['concept_name_id']
    
          arv_regimen_type_TDF_3TC_LPV_r_record = Connection.select_one("SELECT concept_name.concept_name_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'TDF/3TC+LPV/r' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          arv_regimen_type_TDF_3TC_LPV_r = arv_regimen_type_TDF_3TC_LPV_r_record['concept_name_id']
    
          arv_regimen_type_d4T_3TC_d4T_3TC_NVP_record = Connection.select_one("SELECT concept_name.concept_name_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'd4T/3TC + d4T/3TC/NVP (Starter pack)' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          arv_regimen_type_d4T_3TC_d4T_3TC_NVP = arv_regimen_type_d4T_3TC_d4T_3TC_NVP_record['concept_name_id']
    
          arv_regimen_type_AZT_3TC_AZT_3TC_NVP_record = Connection.select_one("SELECT concept_name.concept_name_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'AZT/3TC + AZT/3TC/NVP (Starter pack)' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          arv_regimen_type_AZT_3TC_AZT_3TC_NVP = arv_regimen_type_AZT_3TC_AZT_3TC_NVP_record['concept_name_id']

          case value_coded_name_id
          when arv_regimen_type_AZT_3TC_AZT_3TC_NVP
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, arv_regimen_type_AZT_3TC_AZT_3TC_NVP, arv_regimen_type_AZT_3TC_AZT_3TC_NVP_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_AZT_3TC_AZT_3TC_NVP): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET arv_regimen_type_AZT_3TC_AZT_3TC_NVP = "#{value}", arv_regimen_type_AZT_3TC_AZT_3TC_NVP_enc_id = encounter_id WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_AZT_3TC_AZT_3TC_NVP): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
                            UPDATE flat_table2 SET arv_regimen_type_AZT_3TC_AZT_3TC_NVP = NULL, arv_regimen_type_AZT_3TC_AZT_3TC_NVP_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_AZT_3TC_AZT_3TC_NVP): #{patient_id}"

            end #voided

          when arv_regimen_type_d4T_3TC_d4T_3TC_NVP
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, arv_regimen_type_d4T_3TC_d4T_3TC_NVP, arv_regimen_type_d4T_3TC_d4T_3TC_NVP_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_d4T_3TC_d4T_3TC_NVP): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET arv_regimen_type_d4T_3TC_d4T_3TC_NVP = "#{value}", arv_regimen_type_d4T_3TC_d4T_3TC_NVP_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_d4T_3TC_d4T_3TC_NVP): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET arv_regimen_type_d4T_3TC_d4T_3TC_NVP = NULL, arv_regimen_type_d4T_3TC_d4T_3TC_NVP_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_d4T_3TC_d4T_3TC_NVP): #{patient_id}"

            end #voided

          when arv_regimen_type_TDF_3TC_LPV_r
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, arv_regimen_type_TDF_3TC_LPV_r, arv_regimen_type_TDF_3TC_LPV_r_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_TDF_3TC_LPV_r): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET arv_regimen_type_TDF_3TC_LPV_r = "#{value}", arv_regimen_type_TDF_3TC_LPV_r_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_TDF_3TC_LPV_r): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET arv_regimen_type_TDF_3TC_LPV_r = NULL, arv_regimen_type_TDF_3TC_LPV_r_enc_id = NULL WHERE flat_table2.id = "#{visit}";            
EOF
            end #voided

          when arv_regimen_type_ABC_3TC_LPV_r
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, arv_regimen_type_ABC_3TC_LPV_r, arv_regimen_type_ABC_3TC_LPV_r_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_ABC_3TC_LPV_r): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET arv_regimen_type_ABC_3TC_LPV_r = "#{value}", arv_regimen_type_ABC_3TC_LPV_r_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_ABC_3TC_LPV_r): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET arv_regimen_type_ABC_3TC_LPV_r = NULL, arv_regimen_type_ABC_3TC_LPV_r_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_ABC_3TC_LPV_r): #{patient_id}"

            end #voided

          when arv_regimen_type_TDF_3TC_EFV
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, arv_regimen_type_TDF_3TC_EFV, arv_regimen_type_TDF_3TC_EFV_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_TDF_3TC_EFV): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET arv_regimen_type_TDF_3TC_EFV = "#{value}", arv_regimen_type_TDF_3TC_EFV_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_TDF_3TC_EFV): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET arv_regimen_type_TDF_3TC_EFV = NULL, arv_regimen_type_TDF_3TC_EFV_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
            end #voided

          when arv_regimen_type_TDF_3TC_NVP
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, arv_regimen_type_TDF_3TC_NVP, arv_regimen_type_TDF_3TC_NVP_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_TDF_3TC_NVP): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET arv_regimen_type_TDF_3TC_NVP = "#{value}", arv_regimen_type_TDF_3TC_NVP_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_TDF_3TC_NVP): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET arv_regimen_type_TDF_3TC_NVP = NULL, arv_regimen_type_TDF_3TC_NVP_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
            end #voided

          when arv_regimen_type_d4T_3TC_EFV
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, arv_regimen_type_d4T_3TC_EFV, arv_regimen_type_d4T_3TC_EFV_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_d4T_3TC_EFV): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET arv_regimen_type_d4T_3TC_EFV = "#{value}", arv_regimen_type_d4T_3TC_EFV_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_d4T_3TC_EFV): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET arv_regimen_type_d4T_3TC_EFV = NULL, arv_regimen_type_d4T_3TC_EFV_enc_id = NULL WHERE flat_table2.id = in_visit_id;
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_d4T_3TC_EFV): #{patient_id}"

            end #voided
          when arv_regimen_type_AZT_3TC_EFV
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, arv_regimen_type_AZT_3TC_EFV, arv_regimen_type_AZT_3TC_EFV_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_AZT_3TC_EFV): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET arv_regimen_type_AZT_3TC_EFV = "#{value}", arv_regimen_type_AZT_3TC_EFV_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_AZT_3TC_EFV): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET arv_regimen_type_AZT_3TC_EFV = NULL, arv_regimen_type_AZT_3TC_EFV_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
            end #voided

          when arv_regimen_type_AZT_3TC_LPV_r
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, arv_regimen_type_AZT_3TC_LPV_r, arv_regimen_type_AZT_3TC_LPV_r_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_AZT_3TC_LPV_r): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET arv_regimen_type_AZT_3TC_LPV_r = "#{value}", arv_regimen_type_AZT_3TC_LPV_r_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_AZT_3TC_LPV_r): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET arv_regimen_type_AZT_3TC_LPV_r = NULL, arv_regimen_type_AZT_3TC_LPV_r_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_AZT_3TC_LPV_r): #{patient_id}"

            end #voided

          when arv_regimen_type_AZT_3TC_NVP
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, arv_regimen_type_AZT_3TC_NVP, arv_regimen_type_AZT_3TC_NVP_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_AZT_3TC_NVP): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET arv_regimen_type_AZT_3TC_NVP = "#{value}", arv_regimen_type_AZT_3TC_NVP_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_AZT_3TC_NVP): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET arv_regimen_type_AZT_3TC_NVP = NULL, arv_regimen_type_AZT_3TC_NVP_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_AZT_3TC_NVP): #{patient_id}"

            end #voided

          when arv_regimen_type_triomune_40
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, arv_regimen_type_triomune_40, arv_regimen_type_triomune_40_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_triomune_40): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET arv_regimen_type_triomune_40 = "#{value}", arv_regimen_type_triomune_40_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_triomune_40): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET arv_regimen_type_triomune_40 = NULL, arv_regimen_type_triomune_40_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
            end #voided

          when arv_regimen_type_triomune_30
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, arv_regimen_type_triomune_30, arv_regimen_type_triomune_30_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_triomune_30): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET arv_regimen_type_triomune_30 = "#{value}", arv_regimen_type_triomune_30_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_triomune_30): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET arv_regimen_type_triomune_30 = NULL, arv_regimen_type_triomune_30_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_triomune_30): #{patient_id}"

            end #voided

          when arv_regimen_type_triomune
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, arv_regimen_type_triomune, arv_regimen_type_triomune_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_triomune): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET arv_regimen_type_triomune = @value, arv_regimen_type_triomune_enc_id = encounter_id WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_triomune): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET arv_regimen_type_triomune = NULL, arv_regimen_type_triomune_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_triomune): #{patient_id}"

            end #voided

          when arv_regimen_type_d4T_3TC_NVP
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, arv_regimen_type_d4T_3TC_NVP, arv_regimen_type_d4T_3TC_NVP_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_d4T_3TC_NVP): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET arv_regimen_type_d4T_3TC_NVP = "#{value}", arv_regimen_type_d4T_3TC_NVP_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_d4T_3TC_NVP): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET arv_regimen_type_d4T_3TC_NVP = NULL, arv_regimen_type_d4T_3TC_NVP_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_d4T_3TC_NVP): #{patient_id}"

            end #voided

          when arv_regimen_type_unknown
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, arv_regimen_type_unknown, arv_regimen_type_unknown_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_unknown): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET arv_regimen_type_unknown = "#{value}", arv_regimen_type_unknown_enc_id = encounter_id WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_unknown): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET arv_regimen_type_unknown = NULL, arv_regimen_type_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (arv_regimen_type: value_coded_name_id==arv_regimen_type_unknown): #{patient_id}"

            end #voided
          end #case

        when pregnant
          yes_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Yes' AND voided = 0 AND retired = 0 ")
          yes = yes_record['concept_id']

          no_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'No' AND voided = 0 AND retired = 0 ")
          no = no_record['concept_id']

          unknown = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Unknown' AND voided = 0 AND retired = 0 ")
          case value_coded
          when yes
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            encounter_type_record = Connection.select_one("SELECT * FROM encounter e WHERE e.encounter_id = #{encounter_id} AND voided = #{voided}")
            encounter_type = encounter_type_record['encounter_type']

            if visit.blank?
              if (encounter_type == 52)
                Connection.execute <<EOF
                INSERT INTO flat_table1 (patient_id, pregnant_yes, pregnant_yes_enc_id, pregnant_yes_v_date) VALUES ("#{patient_id}", "#{value}", "#{encounter_id}", '#{Current_date}');
EOF
                puts "........... Inserting record into flat_table1 (pregnant: value_coded==yes; encounter_type == 52): #{patient_id}"

              elsif (encounter_type == 53)
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, pregnant_yes, pregnant_yes_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (pregnant: value_coded==yes; encounter_type == 53): #{patient_id}"

              end #encounter_type

            else
              if (encounter_type == 52)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET pregnant_yes = "#{value}", pregnant_no = NULL, pregnant_unknown = NULL, pregnant_yes_enc_id = "#{encounter_id}", pregnant_no_enc_id = NULL, pregnant_unknown_enc_id = NULL, pregnant_yes_v_date = '#{Current_date}', pregnant_no_v_date = NULL, pregnant_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table1 (pregnant: value_coded==yes; encounter_type == 52): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET pregnant_yes = NULL, pregnant_no = NULL, pregnant_unknown = NULL, pregnant_yes_enc_id = NULL, pregnant_no_enc_id = NULL, pregnant_unknown_enc_id = NULL, pregnant_yes_v_date = NULL, pregnant_no_v_date = NULL, pregnant_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table1 (pregnant: value_coded==yes; encounter_type == 52): #{patient_id}"

                end #voided

              elsif (encounter_type == 53)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET pregnant_yes = "#{value}", pregnant_no = NULL, pregnant_unknown = NULL, pregnant_yes_enc_id = "#{encounter_id}", pregnant_no_enc_id = NULL, pregnant_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table2 (pregnant: value_coded==yes; encounter_type == 53): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET pregnant_yes = NULL, pregnant_no = NULL, pregnant_unknown = NULL, pregnant_yes_enc_id = NULL, pregnant_no_enc_id = NULL, pregnant_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table2 (pregnant: value_coded==yes; encounter_type == 53): #{patient_id}"

                end
              end #encounter_type
            end #visit

          when no
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            encounter_type_record = Connection.select_one("SELECT * FROM encounter e WHERE e.encounter_id = #{encounter_id} AND voided = #{voided}")
            encounter_type = encounter_type_record['encounter_type']

            if visit.blank?
              if (encounter_type == 52)
                Connection.execute <<EOF
                INSERT INTO flat_table1 (patient_id, pregnant_no, pregnant_no_enc_id, pregnant_no_v_date) VALUES ("#{patient_id}", "#{value}", "#{encounter_id}", '#{Current_date}');
EOF
                puts "........... Inserting record into flat_table1 (pregnant: value_coded==no; encounter_type == 52): #{patient_id}"

              elsif (encounter_type == 53)
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, pregnant_no, pregnant_no_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (pregnant: value_coded==no; encounter_type == 53): #{patient_id}"

              end #encouter_type

            else
              if (encounter_type == 52)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET pregnant_no = "#{value}", pregnant_yes = NULL, pregnant_unknown = NULL, pregnant_no_enc_id = "#{encounter_id}", pregnant_yes_enc_id = NULL, pregnant_unknown_enc_id = NULL, pregnant_no_v_date = '#{Current_date}', pregnant_yes_v_date = NULL, pregnant_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table1 (pregnant: value_coded==no; encounter_type == 52): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET pregnant_no = NULL, pregnant_yes = NULL, pregnant_unknown = NULL, pregnant_no_enc_id = NULL, pregnant_yes_enc_id = NULL, pregnant_unknown_enc_id = NULL, pregnant_no_v_date = NULL, pregnant_yes_v_date = NULL, pregnant_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table1 (pregnant: value_coded==no; encounter_type == 52): #{patient_id}"

                end #voided

              elsif (encounter_type == 53)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET pregnant_no = "#{value}", pregnant_yes = NULL, pregnant_unknown = NULL, pregnant_no_enc_id = encounter_id, pregnant_yes_enc_id = NULL, pregnant_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table2 (pregnant: value_coded==no; encounter_type == 53): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET pregnant_no = NULL, pregnant_yes = NULL, pregnant_unknown = NULL, pregnant_no_enc_id = NULL, pregnant_yes_enc_id = NULL, pregnant_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table2 (pregnant: value_coded==no; encounter_type == 53): #{patient_id}"

                end #voided
              end #encounter_type
            end #visit

          when unknown
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            encounter_type_record = Connection.select_one("SELECT * FROM encounter e WHERE e.encounter_id = #{encounter_id} AND voided = #{voided}")
            encounter_type = encounter_type_record['encounter_type']

            if visit.blank?
              if (encounter_type == 52)
                Connection.execute <<EOF
                INSERT INTO flat_table1 (patient_id, pregnant_unknown, pregnant_unknown_enc_id, pregnant_unknown_v_date) VALUES ("#{patient_id}", "#{value}", "#{encounter_id}", '#{Current_date}');
EOF
                puts "........... Inserting record into flat_table1 (pregnant: value_coded==unknown; encounter_type == 52): #{patient_id}"

              elsif (encounter_type == 53)
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, pregnant_unknown, pregnant_unknown_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (pregnant: value_coded==unknown; encounter_type == 52): #{patient_id}"
              end #encounter

            else
              if (encounter_type == 52)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET pregnant_unknown = "#{value}", pregnant_yes = NULL, pregnant_no = NULL, pregnant_unknown_enc_id = "#{encounter_id}", pregnant_yes_enc_id = NULL, pregnant_no_enc_id = NULL, pregnant_unknown_v_date = '#{Current_date}', pregnant_yes_v_date = NULL, pregnant_no_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table1 (pregnant: value_coded==unknown; encounter_type == 52): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET pregnant_unknown = NULL, pregnant_yes = NULL, pregnant_no = NULL, pregnant_unknown_enc_id = NULL, pregnant_yes_enc_id = NULL, pregnant_no_enc_id = NULL, pregnant_unknown_v_date = NULL, pregnant_yes_v_date = NULL, pregnant_no_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table1 (pregnant: value_coded==unknown; encounter_type == 52): #{patient_id}"

                end #voided

              elsif (encounter_type == 53)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET pregnant_unknown = "#{value}", pregnant_yes = NULL, pregnant_no = NULL, pregnant_unknown_enc_id = "#{encounter_id}", pregnant_yes_enc_id = NULL, pregnant_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table2 (pregnant: value_coded==unknown; encounter_type == 53): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET pregnant_unknown = NULL, pregnant_yes = NULL, pregnant_no = NULL, pregnant_unknown_enc_id = NULL, pregnant_yes_enc_id = NULL, pregnant_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table2 (pregnant: value_coded==unknown; encounter_type == 53): #{patient_id}"

                end #voided
              end #encounter_type
            end #visit
          end #case


        when pregnant2
          yes_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Yes' AND voided = 0 AND retired = 0 ")
          yes = yes_record['concept_id']

          no_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'No' AND voided = 0 AND retired = 0 ")
          no = no_record['concept_id']

          unknown = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Unknown' AND voided = 0 AND retired = 0 ")
          case value_coded
          when yes
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            encounter_type_record = Connection.select_one("SELECT * FROM encounter e WHERE e.encounter_id = #{encounter_id} AND voided = #{voided}")
            encounter_type = encounter_type_record['encounter_type']

            if visit.blank?
              if (encounter_type == 52)
                Connection.execute <<EOF
                INSERT INTO flat_table1 (patient_id, pregnant_yes, pregnant_yes_enc_id, pregnant_yes_v_date) VALUES ("#{patient_id}", "#{value}", "#{encounter_id}", '#{Current_date}');
EOF
                puts "........... Inserting record into flat_table1 (pregnant: value_coded==yes; encounter_type == 52): #{patient_id}"

              elsif (encounter_type == 53)
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, pregnant_yes, pregnant_yes_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (pregnant: value_coded==yes; encounter_type == 53): #{patient_id}"

              end #encounter_type

            else
              if (encounter_type == 52)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET pregnant_yes = "#{value}", pregnant_no = NULL, pregnant_unknown = NULL, pregnant_yes_enc_id = "#{encounter_id}", pregnant_no_enc_id = NULL, pregnant_unknown_enc_id = NULL, pregnant_yes_v_date = '#{Current_date}', pregnant_no_v_date = NULL, pregnant_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table1 (pregnant: value_coded==yes; encounter_type == 52): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET pregnant_yes = NULL, pregnant_no = NULL, pregnant_unknown = NULL, pregnant_yes_enc_id = NULL, pregnant_no_enc_id = NULL, pregnant_unknown_enc_id = NULL, pregnant_yes_v_date = NULL, pregnant_no_v_date = NULL, pregnant_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table1 (pregnant: value_coded==yes; encounter_type == 52): #{patient_id}"

                end #voided

              elsif (encounter_type == 53)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET pregnant_yes = "#{value}", pregnant_no = NULL, pregnant_unknown = NULL, pregnant_yes_enc_id = "#{encounter_id}", pregnant_no_enc_id = NULL, pregnant_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table2 (pregnant: value_coded==yes; encounter_type == 53): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET pregnant_yes = NULL, pregnant_no = NULL, pregnant_unknown = NULL, pregnant_yes_enc_id = NULL, pregnant_no_enc_id = NULL, pregnant_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table2 (pregnant: value_coded==yes; encounter_type == 53): #{patient_id}"

                end
              end #encounter_type
            end #visit

          when no
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            encounter_type_record = Connection.select_one("SELECT * FROM encounter e WHERE e.encounter_id = #{encounter_id} AND voided = #{voided}")
            encounter_type = encounter_type_record['encounter_type']

            if visit.blank?
              if (encounter_type == 52)
                Connection.execute <<EOF
                INSERT INTO flat_table1 (patient_id, pregnant_no, pregnant_no_enc_id, pregnant_no_v_date) VALUES ("#{patient_id}", "#{value}", "#{encounter_id}", '#{Current_date}');
EOF
                puts "........... Inserting record into flat_table1 (pregnant: value_coded==no; encounter_type == 52): #{patient_id}"

              elsif (encounter_type == 53)
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, pregnant_no, pregnant_no_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (pregnant: value_coded==no; encounter_type == 53): #{patient_id}"

              end #encouter_type

            else
              if (encounter_type == 52)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET pregnant_no = "#{value}", pregnant_yes = NULL, pregnant_unknown = NULL, pregnant_no_enc_id = "#{encounter_id}", pregnant_yes_enc_id = NULL, pregnant_unknown_enc_id = NULL, pregnant_no_v_date = '#{Current_date}', pregnant_yes_v_date = NULL, pregnant_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table1 (pregnant: value_coded==no; encounter_type == 52): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET pregnant_no = NULL, pregnant_yes = NULL, pregnant_unknown = NULL, pregnant_no_enc_id = NULL, pregnant_yes_enc_id = NULL, pregnant_unknown_enc_id = NULL, pregnant_no_v_date = NULL, pregnant_yes_v_date = NULL, pregnant_unknown_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table1 (pregnant: value_coded==no; encounter_type == 52): #{patient_id}"

                end #voided

              elsif (encounter_type == 53)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET pregnant_no = "#{value}", pregnant_yes = NULL, pregnant_unknown = NULL, pregnant_no_enc_id = encounter_id, pregnant_yes_enc_id = NULL, pregnant_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table2 (pregnant: value_coded==no; encounter_type == 53): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET pregnant_no = NULL, pregnant_yes = NULL, pregnant_unknown = NULL, pregnant_no_enc_id = NULL, pregnant_yes_enc_id = NULL, pregnant_unknown_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table2 (pregnant: value_coded==no; encounter_type == 53): #{patient_id}"

                end #voided
              end #encounter_type
            end #visit

          when unknown
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            encounter_type_record = Connection.select_one("SELECT * FROM encounter e WHERE e.encounter_id = #{encounter_id} AND voided = #{voided}")
            encounter_type = encounter_type_record['encounter_type']

            if visit.blank?
              if (encounter_type == 52)
                Connection.execute <<EOF
                INSERT INTO flat_table1 (patient_id, pregnant_unknown, pregnant_unknown_enc_id, pregnant_unknown_v_date) VALUES ("#{patient_id}", "#{value}", "#{encounter_id}", '#{Current_date}');
EOF
                puts "........... Inserting record into flat_table1 (pregnant: value_coded==unknown; encounter_type == 52): #{patient_id}"

              elsif (encounter_type == 53)
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, pregnant_unknown, pregnant_unknown_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (pregnant: value_coded==unknown; encounter_type == 52): #{patient_id}"
              end #encounter

            else
              if (encounter_type == 52)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET pregnant_unknown = "#{value}", pregnant_yes = NULL, pregnant_no = NULL, pregnant_unknown_enc_id = "#{encounter_id}", pregnant_yes_enc_id = NULL, pregnant_no_enc_id = NULL, pregnant_unknown_v_date = '#{Current_date}', pregnant_yes_v_date = NULL, pregnant_no_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table1 (pregnant: value_coded==unknown; encounter_type == 52): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET pregnant_unknown = NULL, pregnant_yes = NULL, pregnant_no = NULL, pregnant_unknown_enc_id = NULL, pregnant_yes_enc_id = NULL, pregnant_no_enc_id = NULL, pregnant_unknown_v_date = NULL, pregnant_yes_v_date = NULL, pregnant_no_v_date = NULL WHERE flat_table1.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table1 (pregnant: value_coded==unknown; encounter_type == 52): #{patient_id}"

                end #voided

              elsif (encounter_type == 53)
                if voided.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET pregnant_unknown = "#{value}", pregnant_yes = NULL, pregnant_no = NULL, pregnant_unknown_enc_id = "#{encounter_id}", pregnant_yes_enc_id = NULL, pregnant_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table2 (pregnant: value_coded==unknown; encounter_type == 53): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET pregnant_unknown = NULL, pregnant_yes = NULL, pregnant_no = NULL, pregnant_unknown_enc_id = NULL, pregnant_yes_enc_id = NULL, pregnant_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                  puts "........... Updating record into flat_table2 (pregnant: value_coded==unknown; encounter_type == 53): #{patient_id}"

                end #voided
              end #encounter_type
            end #visit
          end #case


        when method_of_family_planning
          family_planning_method_oral_contraceptive_pills_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Oral contraceptive pills' AND voided = 0 AND retired = 0 ")
          family_planning_method_oral_contraceptive_pills = family_planning_method_oral_contraceptive_pills_record['concept_id']
    
          family_planning_method_depo_provera_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Depo-provera' AND voided = 0 AND retired = 0")
          family_planning_method_depo_provera = family_planning_method_depo_provera_record['concept_id']
    
          family_planning_method_intrauterine_contraception_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Intrauterine contraception' AND voided = 0 AND retired = 0 ")
          family_planning_method_intrauterine_contraception = family_planning_method_intrauterine_contraception_record['concept_id']
    
          family_planning_method_contraceptive_implant_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Contraceptive implant' AND voided = 0 AND retired = 0 ")
          family_planning_method_contraceptive_implant = family_planning_method_contraceptive_implant_record['concept_id']
    
          family_planning_method_male_condoms_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Male condoms' AND voided = 0 AND retired = 0 ")
          family_planning_method_male_condoms = family_planning_method_male_condoms_record['concept_id']
    
          family_planning_method_female_condoms_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Female condoms' AND voided = 0 AND retired = 0 ")
          family_planning_method_female_condoms = family_planning_method_female_condoms_record['concept_id']
    
          family_planning_method_rythm_method_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Rhythm method' AND voided = 0 AND retired = 0 ")
          family_planning_method_rythm_method = family_planning_method_rythm_method_record['concept_id']
    
          family_planning_method_withdrawal_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Withdrawal method' AND voided = 0 AND retired = 0 ")
          family_planning_method_withdrawal = family_planning_method_withdrawal_record['concept_id']
    
          family_planning_method_abstinence_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Abstinence' AND voided = 0 AND retired = 0 ")
          family_planning_method_abstinence = family_planning_method_abstinence_record['concept_id']
    
          family_planning_method_tubal_ligation_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Tubal ligation' AND voided = 0 AND retired = 0 ")
          family_planning_method_tubal_ligation = family_planning_method_tubal_ligation_record['concept_id']
    
          family_planning_method_vasectomy_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Vasectomy' AND voided = 0 AND retired = 0 ")
          family_planning_method_vasectomy = family_planning_method_vasectomy_record['concept_id']
    
          family_planning_method_emergency_contraception_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Emergency contraception' AND voided = 0 AND retired = 0 ")
          family_planning_method_emergency_contraception = family_planning_method_emergency_contraception_record['concept_id']

          case value_coded
          when family_planning_method_oral_contraceptive_pills
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, family_planning_method_oral_contraceptive_pills, family_planning_method_oral_contraceptive_pills_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_oral_contraceptive_pills): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET family_planning_method_oral_contraceptive_pills = "#{value}", family_planning_method_oral_contraceptive_pills_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_oral_contraceptive_pills): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET family_planning_method_oral_contraceptive_pills = NULL, family_planning_method_oral_contraceptive_pills_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_oral_contraceptive_pills): #{patient_id}"

            end #voided

          when family_planning_method_depo_provera
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, family_planning_method_intrauterine_contraception, family_planning_method_intrauterine_contraception_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_depo_provera): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET family_planning_method_depo_provera = "#{value}", family_planning_method_depo_provera_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_depo_provera): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET family_planning_method_depo_provera = NULL, family_planning_method_depo_provera_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_depo_provera): #{patient_id}"

            end #voided

          when family_planning_method_intrauterine_contraception
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, family_planning_method_intrauterine_contraception, family_planning_method_intrauterine_contraception_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_intrauterine_contraception): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET family_planning_method_intrauterine_contraception = "#{value}", family_planning_method_intrauterine_contraception_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_intrauterine_contraception): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET family_planning_method_intrauterine_contraception = NULL, family_planning_method_intrauterine_contraception_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_intrauterine_contraception): #{patient_id}"

            end #voided

          when family_planning_method_contraceptive_implant
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, family_planning_method_contraceptive_implant, family_planning_method_contraceptive_implant_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_contraceptive_implant): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET family_planning_method_contraceptive_implant = "#{value}", family_planning_method_contraceptive_implant_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_contraceptive_implant): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET family_planning_method_contraceptive_implant = NULL, family_planning_method_contraceptive_implant_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_contraceptive_implant): #{patient_id}"

            end #voided

          when family_planning_method_male_condoms
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, family_planning_method_male_condoms, family_planning_method_male_condoms_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_male_condoms): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET family_planning_method_male_condoms = "#{value}", family_planning_method_male_condoms_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_male_condoms): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET family_planning_method_male_condoms = NULL, family_planning_method_male_condoms_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_male_condoms): #{patient_id}"

            end #voided

          when family_planning_method_female_condoms
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, family_planning_method_female_condoms, family_planning_method_female_condoms_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_female_condoms): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET family_planning_method_female_condoms = @value, family_planning_method_female_condoms_enc_id = encounter_id WHERE flat_table2.id = in_visit_id;
EOF
                puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_female_condoms): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET family_planning_method_female_condoms = NULL, family_planning_method_female_condoms_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_female_condoms): #{patient_id}"

            end #voided

          when family_planning_metho_rythm_method
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, family_planning_method_rythm_method, family_planning_method_rythm_method_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (method_of_family_planning: value_coded==family_planning_metho_rythm_method): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET family_planning_method_rythm_method = "#{value}", family_planning_method_rythm_method_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_metho_rythm_method): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET family_planning_method_rythm_method = NULL, family_planning_method_rythm_method_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_metho_rythm_method): #{patient_id}"

            end #voided

          when family_planning_method_withdrawal
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, family_planning_method_withdrawal, family_planning_method_withdrawal_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_withdrawal): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET family_planning_method_withdrawal = "#{value}", family_planning_method_withdrawal_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_withdrawal): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET family_planning_method_withdrawal = NULL, family_planning_method_withdrawal_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_withdrawal): #{patient_id}"

            end #voided

          when family_planning_method_abstinence
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, family_planning_method_abstinence, family_planning_method_abstinence_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_abstinence): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET family_planning_method_abstinence = "#{value}", family_planning_method_abstinence_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_abstinence): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET family_planning_method_abstinence = NULL, family_planning_method_abstinence_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_abstinence): #{patient_id}"

            end #voided

          when family_planning_method_tubal_ligation
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, family_planning_method_tubal_ligation, family_planning_method_tubal_ligation_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_tubal_ligation): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET family_planning_method_tubal_ligation = "#{value}", family_planning_method_tubal_ligation_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_tubal_ligation): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET family_planning_method_tubal_ligation = NULL, family_planning_method_tubal_ligation_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_tubal_ligation): #{patient_id}"

            end #voided

          when family_planning_method_vasectomy
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, family_planning_method_vasectomy, family_planning_method_vasectomy_enc_id) VALUES ("#{patient_id}", in_visit_date, "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_vasectomy): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET family_planning_method_vasectomy = "#{value}", family_planning_method_vasectomy_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_vasectomy): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET family_planning_method_vasectomy = NULL, family_planning_method_vasectomy_enc_id = NULL WHERE flat_table2.id = in_visit_id;
EOF
              puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_vasectomy): #{patient_id}"

            end #voided

          when family_planning_method_emergency_contraception
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, family_planning_method_emergency_contraception, family_planning_method_emergency_contraception_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_emergency_contraception): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET family_planning_method_emergency_contraception = "#{value}", family_planning_method_emergency_contraception_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_emergency_contraception): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET family_planning_method_emergency_contraception = NULL, family_planning_method_emergency_contraception_enc_id = NULL WHERE flat_table2.id = in_visit_id;
EOF
                puts "........... Updating record into flat_table2 (method_of_family_planning: value_coded==family_planning_method_emergency_contraception): #{patient_id}"

            end #voided
          end #case

        when currently_using_family_planning_method
          yes_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Yes' AND voided = 0 AND retired = 0 ")
          yes = yes_record['concept_id']

          no_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'No' AND voided = 0 AND retired = 0 ")
          no = no_record['concept_id']

          case value_coded
          when yes
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, currently_using_family_planning_method_yes, currently_using_family_planning_method_yes_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (currently_using_family_planning_method: value_coded==yes): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET currently_using_family_planning_method_yes = "#{value}", currently_using_family_planning_method_no = NULL, currently_using_family_planning_method_yes_enc_id = "#{encounter_id}", currently_using_family_planning_method_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (currently_using_family_planning_method: value_coded==yes): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET currently_using_family_planning_method_yes = NULL, currently_using_family_planning_method_no = NULL, currently_using_family_planning_method_yes_enc_id = NULL, currently_using_family_planning_method_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (currently_using_family_planning_method: value_coded==yes): #{patient_id}"

            end #voided

          when no
            value_record = Connection.select_one("SELECT * FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, currently_using_family_planning_method_no, currently_using_family_planning_method_no_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (currently_using_family_planning_method: value_coded==no): #{patient_id}"
  
              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET currently_using_family_planning_method_no = "#{value}", currently_using_family_planning_method_yes = NULL, currently_using_family_planning_method_no_enc_id = "#{encounter_id}", currently_using_family_planning_method_yes_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (currently_using_family_planning_method: value_coded==no): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET currently_using_family_planning_method_no = NULL, currently_using_family_planning_method_yes = NULL, currently_using_family_planning_method_no_enc_id = NULL, currently_using_family_planning_method_yes_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (currently_using_family_planning_method: value_coded==no): #{patient_id}"

            end #voided
          end #case

        when symptom_present
          symptom_present_lipodystrophy_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Lipodystrophy' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_lipodystrophy = symptom_present_lipodystrophy_record['concept_id']
    
          symptom_present_anemia_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Anemia' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_anemia = symptom_present_anemia_record['concept_id']
    
          symptom_present_jaundice_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Jaundice' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_jaundice = symptom_present_jaundice_record['concept_id']
    
          symptom_present_lactic_acidosis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Lactic acidosis' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_lactic_acidosis = symptom_present_lactic_acidosis_record['concept_id']
    
          symptom_present_fever_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Fever' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_fever = symptom_present_fever_record['concept_id']

          symptom_present_skin_rash_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Skin rash' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_skin_rash = symptom_present_skin_rash_record['concept_id']
    
          symptom_present_abdominal_pain_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Abdominal pain' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_abdominal_pain = symptom_present_abdominal_pain_record['concept_id']
    
          symptom_present_anorexia_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Anorexia' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_anorexia = symptom_present_anorexia_record['concept_id']
    
          symptom_present_cough_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Cough' AND concept_name_type = 'FULLY_SPECIFIED' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_cough = symptom_present_cough_record['concept_id']
    
          symptom_present_diarrhea_record = ("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Diarrhea' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_diarrhea = symptom_present_diarrhea_record['concept_id']
    
          symptom_present_hepatitis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Hepatitis' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_hepatitis = symptom_present_hepatitis_record['concept_id']
    
          symptom_present_leg_pain_numbness_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Leg pain / numbness' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_leg_pain_numbness = symptom_present_leg_pain_numbness_record['concept_id']
    
          symptom_present_peripheral_neuropathy_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Peripheral neuropathy' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_peripheral_neuropathy = symptom_present_peripheral_neuropathy_record['concept_id']
    
          symptom_present_vomiting_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Vomiting' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_vomiting = symptom_present_vomiting_record['concept_id']
    
          symptom_present_other_symptom_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Other symptom' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_other_symptom = symptom_present_other_symptom_record['concept_id']

          symptom_present_kidney_failure_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Kidney Failure' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_kidney_failure = symptom_present_kidney_failure_record['concept_id']
    
          symptom_present_nightmares_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Nightmares' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_nightmares = symptom_present_nightmares_record['concept_id']
    
          symptom_present_diziness_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Dizziness' AND concept_name_type = 'FULLY_SPECIFIED' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_diziness = symptom_present_diziness_record['concept_id']
    
          symptom_present_psychosis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Psychosis' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_psychosis = symptom_present_psychosis_record['concept_id']
    
          symptom_present_blurry_vision_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Blurry Vision' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          symptom_present_blurry_vision = symptom_present_blurry_vision_record['concept_id']

          case value_coded
          when symptom_present_lipodystrophy
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_lipodystrophy, symptom_present_lipodystrophy_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_lipodystrophy): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_lipodystrophy = 'Yes', symptom_present_lipodystrophy_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_lipodystrophy): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_lipodystrophy = NULL, symptom_present_lipodystrophy_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_lipodystrophy): #{patient_id}"

            end #voided

          when symptom_present_anemia
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_anemia, symptom_present_anemia_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_anemia): #{patient_id}"
              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_anemia = 'Yes', symptom_present_anemia_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_anemia): #{patient_id}"

              end #visit
            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_anemia = NULL, symptom_present_anemia_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_anemia): #{patient_id}"

            end #voided

          when symptom_present_jaundice
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_jaundice, symptom_present_jaundice_enc_id) VALUES ("#{patient_id}", '{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_jaundice): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_jaundice = 'Yes', symptom_present_jaundice_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_jaundice): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_jaundice = NULL, symptom_present_jaundice_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_jaundice): #{patient_id}"

            end #voided

          when symptom_present_lactic_acidosis
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_lactic_acidosis, symptom_present_lactic_acidosis_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_lactic_acidosis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_lactic_acidosis = 'Yes', symptom_present_lactic_acidosis_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_lactic_acidosis): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_lactic_acidosis = NULL, symptom_present_lactic_acidosis_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_lactic_acidosis): #{patient_id}"

            end #voided

          when symptom_present_fever
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_fever, symptom_present_fever_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_fever): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_fever = 'Yes', symptom_present_fever_enc_id = encounter_id WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_fever): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_fever = NULL, symptom_present_fever_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
            end #voided

          when symptom_present_skin_rash
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_skin_rash, symptom_present_skin_rash_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_skin_rash): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_skin_rash = 'Yes', symptom_present_skin_rash_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_skin_rash): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_skin_rash = NULL, symptom_present_skin_rash_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
            end #voided

          when symptom_present_abdominal_pain
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_abdominal_pain, symptom_present_abdominal_pain_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_abdominal_pain): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_abdominal_pain = 'Yes', symptom_present_abdominal_pain_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_abdominal_pain): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_abdominal_pain = NULL, symptom_present_abdominal_pain_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_abdominal_pain): #{patient_id}"

            end #voided

          when symptom_present_anorexia
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 ("patient_id", visit_date, symptom_present_anorexia, symptom_present_anorexia_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_anorexia): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_anorexia = 'Yes', symptom_present_anorexia_enc_id = encounter_id WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_anorexia): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_anorexia = NULL, symptom_present_anorexia_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_anorexia): #{patient_id}"

            end #voided

          when symptom_present_cough
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_cough, symptom_present_cough_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_cough): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_cough = 'Yes', symptom_present_cough_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_cough): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_cough = NULL, symptom_present_cough_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_cough): #{patient_id}"

            end #voided

          when symptom_present_diarrhea
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_diarrhea, symptom_present_diarrhea_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_diarrhea): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_diarrhea = 'Yes', symptom_present_diarrhea_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_diarrhea): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_diarrhea = NULL, symptom_present_diarrhea_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_diarrhea): #{patient_id}"

            end #voided

          when symptom_present_hepatitis
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_hepatitis, symptom_present_hepatitis_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_hepatitis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_hepatitis = 'Yes', symptom_present_hepatitis_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_hepatitis): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_hepatitis = NULL, symptom_present_hepatitis_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_hepatitis): #{patient_id}"

            end #Voided

          when symptom_present_leg_pain_numbness
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_leg_pain_numbness, symptom_present_leg_pain_numbness_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_leg_pain_numbness): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_leg_pain_numbness = 'Yes', symptom_present_leg_pain_numbness_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_leg_pain_numbness): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_leg_pain_numbness = NULL, symptom_present_leg_pain_numbness_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_leg_pain_numbness): #{patient_id}"

            end #voided

          when symptom_present_peripheral_neuropathy
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_peripheral_neuropathy, symptom_present_peripheral_neuropathy_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_peripheral_neuropathy): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_peripheral_neuropathy = 'Yes', symptom_present_peripheral_neuropathy_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_peripheral_neuropathy): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_peripheral_neuropathy = NULL, symptom_present_peripheral_neuropathy_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
            end #voided

          when symptom_present_vomiting
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_vomiting, symptom_present_vomiting_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_vomiting): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_vomiting = 'Yes', symptom_present_vomiting_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_vomiting): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_vomiting = NULL, symptom_present_vomiting_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_vomiting): #{patient_id}"

            end #voided

          when symptom_present_other_symptom
            if voided.blank
              if visit.blank?
                Connection.execute <<EOF
                                  INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_other_symptom, symptom_present_other_symptom_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_other_symptom): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_other_symptom = 'Yes', symptom_present_other_symptom_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_other_symptom): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_other_symptom = NULL, symptom_present_other_symptom_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_other_symptom): #{patient_id}"

            end #voided

          when symptom_present_kidney_failure
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_kidney_failure, symptom_present_kidney_failure_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_kidney_failure): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_kidney_failure = 'Yes', symptom_present_kidney_failure_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_kidney_failure): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_kidney_failure = NULL, symptom_present_kidney_failure_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_kidney_failure): #{patient_id}"

            end #voided

          when symptom_present_nightmares
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_nightmares, symptom_present_nightmares_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_nightmares): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_nightmares = 'Yes', symptom_present_nightmares_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_nightmares): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_nightmares = NULL, symptom_present_nightmares_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_nightmares): #{patient_id}"

            end #voided

          when symptom_present_diziness
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_diziness, symptom_present_diziness_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_diziness): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_diziness = 'Yes', symptom_present_diziness_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_diziness): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_diziness = NULL, symptom_present_diziness_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_diziness): #{patient_id}"

            end #voided

          when symptom_present_psychosis
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_psychosis, symptom_present_psychosis_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_psychosis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_psychosis = 'Yes', symptom_present_psychosis_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_psychosis): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_psychosis = NULL, symptom_present_psychosis_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_psychosis): #{patient_id}"

            end #voided

          when symptom_present_blurry_vision
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, symptom_present_blurry_vision, symptom_present_blurry_vision_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (symptom_present: value_coded==symptom_present_blurry_vision): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET symptom_present_blurry_vision = 'Yes', symptom_present_blurry_vision_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_blurry_vision): #{patient_id}"

              end #visit
            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET symptom_present_blurry_vision = NULL, symptom_present_blurry_vision_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (symptom_present: value_coded==symptom_present_blurry_vision): #{patient_id}"

            end #voided
          end #case

        when drug_induced
          drug_induced_lipodystrophy_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Lipodystrophy' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_lipodystrophy = drug_induced_lipodystrophy_record['concept_id']

          drug_induced_anemia_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Anemia' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_anemia = drug_induced_anemia_record['concept_id']

          drug_induced_jaundice_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Jaundice' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_jaundice = drug_induced_jaundice_record['concept_id']

          drug_induced_lactic_acidosis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Lactic acidosis' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_lactic_acidosis = drug_induced_lactic_acidosis_record['concept_id']

          drug_induced_fever_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Fever' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_fever = drug_induced_fever_record['concept_id']

          drug_induced_skin_rash_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Skin rash' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_skin_rash = drug_induced_skin_rash_record['concept_id']

          drug_induced_abdominal_pain_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Abdominal pain' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_abdominal_pain = drug_induced_abdominal_pain_record['concept_id']

          drug_induced_anorexia_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Anorexia' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_anorexia = drug_induced_anorexia_record['concept_id']

          drug_induced_cough_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Cough' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_cough = drug_induced_cough_record['concept_id']

          drug_induced_diarrhea_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Diarrhea' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_diarrhea = drug_induced_diarrhea_record['concept_id']

          drug_induced_hepatitis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Hepatitis' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_hepatitis = drug_induced_hepatitis_record['concept_id']

          drug_induced_leg_pain_numbness_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Leg pain / numbness' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_leg_pain_numbness = drug_induced_leg_pain_numbness_record['concept_id']

          drug_induced_peripheral_neuropathy_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Peripheral neuropathy' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_peripheral_neuropathy = drug_induced_peripheral_neuropathy_record['concept_id']

          drug_induced_vomiting_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Vomiting' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_vomiting = drug_induced_vomiting_record['concept_id']

          drug_induced_other_symptom_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Other symptom' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_other_symptom = drug_induced_other_symptom_record['concept_id']

          drug_induced_kidney_failure_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Kidney Failure ' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_kidney_failure = drug_induced_kidney_failure_record['concept_id']

          drug_induced_nightmares_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Nightmares' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_nightmares = drug_induced_nightmares_record['concept_id']

          drug_induced_diziness_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Dizziness' AND concept_name_type = 'FULLY_SPECIFIED' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_diziness = drug_induced_diziness_record

          drug_induced_psychosis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Psychosis' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          drug_induced_psychosis = drug_induced_psychosis_record['concept_id']

          drug_induced_blurry_vision_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Blurry Vision' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")

          case value_coded
          when drug_induced_lipodystrophy
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_lipodystrophy, drug_induced_lipodystrophy_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_lipodystrophy): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_lipodystrophy = 'Yes', drug_induced_lipodystrophy_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_lipodystrophy): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_lipodystrophy = NULL, drug_induced_lipodystrophy_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_lipodystrophy): #{patient_id}"

            end #voided

          when drug_induced_anemia
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_anemia, drug_induced_anemia_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_anemia): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_anemia = 'Yes', drug_induced_anemia_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_anemia): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_anemia = NULL, drug_induced_anemia_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_anemia): #{patient_id}"

            end #voided

          when drug_induced_jaundice
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_jaundice, drug_induced_jaundice_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_jaundice): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_jaundice = 'Yes', drug_induced_jaundice_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_jaundice): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_jaundice = NULL, drug_induced_jaundice_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_jaundice): #{patient_id}"

            end #voided

          when drug_induced_lactic_acidosis
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_lactic_acidosis, drug_induced_lactic_acidosis_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_lactic_acidosis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_lactic_acidosis = 'Yes', drug_induced_lactic_acidosis_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_lactic_acidosis): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_lactic_acidosis = NULL, drug_induced_lactic_acidosis_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_lactic_acidosis): #{patient_id}"

            end #voided

          when drug_induced_fever
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_fever, drug_induced_fever_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
              puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_fever): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_fever = 'Yes', drug_induced_fever_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_fever): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_fever = NULL, drug_induced_fever_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_fever): #{patient_id}"

            end #voided

          when drug_induced_skin_rash
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_skin_rash, drug_induced_skin_rash_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_skin_rash): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_skin_rash = 'Yes', drug_induced_skin_rash_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_skin_rash): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_skin_rash = NULL, drug_induced_skin_rash_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_skin_rash): #{patient_id}"

            end #voided

          when drug_induced_abdominal_pain
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_abdominal_pain, drug_induced_abdominal_pain_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_abdominal_pain): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_abdominal_pain = 'Yes', drug_induced_abdominal_pain_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_abdominal_pain): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_abdominal_pain = NULL, drug_induced_abdominal_pain_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
            end #voided

          when drug_induced_anorexia
            if voided.blank?
              if visit.blank
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_anorexia, drug_induced_anorexia_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_anorexia): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_anorexia = 'Yes', drug_induced_anorexia_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_anorexia): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_anorexia = NULL, drug_induced_anorexia_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_anorexia): #{patient_id}"

            end #voided

          when drug_induced_cough
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_cough, drug_induced_cough_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_cough): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_cough = 'Yes', drug_induced_cough_enc_id = encounter_id WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_cough): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_cough = NULL, drug_induced_cough_enc_id = NULL WHERE flat_table2.id = "#{in_visit_id}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_cough): #{patient_id}"

            end #voided

          when drug_induced_diarrhea
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_diarrhea, drug_induced_diarrhea_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_diarrhea): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_diarrhea = 'Yes', drug_induced_diarrhea_enc_id = encounter_id WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_diarrhea): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_diarrhea = NULL, drug_induced_diarrhea_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_diarrhea): #{patient_id}"

            end #voided

          when drug_induced_hepatitis
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_hepatitis, drug_induced_hepatitis_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_hepatitis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_hepatitis = 'Yes', drug_induced_hepatitis_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_hepatitis): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_hepatitis = NULL, drug_induced_hepatitis_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_hepatitis): #{patient_id}"

            end #voided

          when drug_induced_leg_pain_numbness
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_leg_pain_numbness, drug_induced_leg_pain_numbness_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_leg_pain_numbness): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_leg_pain_numbness = 'Yes', drug_induced_leg_pain_numbness_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_leg_pain_numbness): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_leg_pain_numbness = NULL, drug_induced_leg_pain_numbness_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_leg_pain_numbness): #{patient_id}"

            end #voided

          when drug_induced_peripheral_neuropathy
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_peripheral_neuropathy, drug_induced_peripheral_neuropathy_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_peripheral_neuropathy): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_peripheral_neuropathy = 'Yes', drug_induced_peripheral_neuropathy_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_peripheral_neuropathy): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_peripheral_neuropathy = NULL, drug_induced_peripheral_neuropathy_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_peripheral_neuropathy): #{patient_id}"

            end #voided

          when drug_induced_vomiting
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_vomiting, drug_induced_vomiting_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_vomiting): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_vomiting = 'Yes', drug_induced_vomiting_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_vomiting): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_vomiting = NULL, drug_induced_vomiting_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_vomiting): #{patient_id}"

            end #voided

          when drug_induced_other_symptom
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_other_symptom, drug_induced_other_symptom_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_other_symptom): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_other_symptom = 'Yes', drug_induced_other_symptom_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_other_symptom): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_other_symptom = NULL, drug_induced_other_symptom_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_other_symptom): #{patient_id}"

            end #voided

          when drug_induced_kidney_failure
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_kidney_failure, drug_induced_kidney_failure_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_kidney_failure): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_kidney_failure = 'Yes', drug_induced_kidney_failure_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_kidney_failure): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_kidney_failure = NULL, drug_induced_kidney_failure_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_kidney_failure): #{patient_id}"

            end #voided

          when drug_induced_nightmares
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_nightmares, drug_induced_nightmares_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_nightmares): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_nightmares = 'Yes', drug_induced_nightmares_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_nightmares): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_nightmares = NULL, drug_induced_nightmares_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_nightmares): #{patient_id}"

            end #voided

          when drug_induced_diziness
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_diziness, drug_induced_diziness_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_diziness): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_diziness = 'Yes', drug_induced_diziness_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_diziness): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_diziness = NULL, drug_induced_diziness_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_diziness): #{patient_id}"

            end #voided

          when drug_induced_psychosis
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_psychosis, drug_induced_psychosis_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_psychosis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_psychosis = 'Yes', drug_induced_psychosis_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_psychosis): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_psychosis = NULL, drug_induced_psychosis_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_psychosis): #{patient_id}"

            end #voided

          when drug_induced_blurry_vision
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, drug_induced_blurry_vision, drug_induced_blurry_vision_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (drug_induced: value_coded==drug_induced_blurry_vision): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET drug_induced_blurry_vision = 'Yes', drug_induced_blurry_vision_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_blurry_vision): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET drug_induced_blurry_vision = NULL, drug_induced_blurry_vision_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (drug_induced: value_coded==drug_induced_blurry_vision): #{patient_id}"

            end #voided
          end #case

        when side_effects
          side_effects_no_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'No' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          side_effects_no = side_effects_no_record['concept_id']
    
          side_effects_peripheral_neuropathy_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Peripheral neuropathy' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          side_effects_peripheral_neuropathy = side_effects_peripheral_neuropathy_record['concept_id']
    
          side_effects_hepatitis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Hepatitis' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          side_effects_hepatitis = side_effects_hepatitis_record['concept_id']
    
          side_effects_skin_rash_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Skin rash' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          side_effects_skin_rash = side_effects_skin_rash_record['concept_id']
    
          side_effects_lipodystrophy_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Lipodystrophy' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          side_effects_lipodystrophy = side_effects_lipodystrophy_record['concept_id']
    
          side_effects_other_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'other' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          side_effects_other = side_effects_other_record['concept_id']
    
          side_effects_kidney_failure_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Kidney Failure ' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          side_effects_kidney_failure = side_effects_kidney_failure_record['concept_id']
    
          side_effects_nightmares_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Nightmares' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          side_effects_nightmares = side_effects_nightmares_record['concept_id']
    
          side_effects_diziness_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Dizziness' AND concept_name_type = 'FULLY_SPECIFIED' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          side_effects_diziness = side_effects_diziness_record['concept_id']
    
          side_effects_psychosis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Psychosis' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          side_effects_psychosis = side_effects_psychosis_record['concept_id']
    
          side_effects_blurry_vision_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Blurry Vision' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")

          case value_coded
          when side_effects_no
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, side_effects_no, side_effects_no_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (side_effects: value_coded==side_effects_no): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET side_effects_no = 'Yes', side_effects_no_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_no): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET side_effects_no = NULL, side_effects_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_no): #{patient_id}"

            end #voided

          when side_effects_peripheral_neuropathy
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, side_effects_peripheral_neuropathy, side_effects_peripheral_neuropathy_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (side_effects: value_coded==side_effects_peripheral_neuropathy): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET side_effects_peripheral_neuropathy = 'Yes', side_effects_peripheral_neuropathy_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_peripheral_neuropathy): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET side_effects_peripheral_neuropathy = NULL, side_effects_peripheral_neuropathy_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_peripheral_neuropathy): #{patient_id}"

            end #voided

          when side_effects_hepatitis
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, side_effects_hepatitis, side_effects_hepatitis_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (side_effects: value_coded==side_effects_hepatitis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET side_effects_hepatitis = 'Yes', side_effects_hepatitis_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_hepatitis): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET side_effects_hepatitis = NULL, side_effects_hepatitis_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_hepatitis): #{patient_id}"

            end #voided

          when side_effects_skin_rash
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, side_effects_skin_rash, side_effects_skin_rash_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (side_effects: value_coded==side_effects_skin_rash): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET side_effects_skin_rash = 'Yes', side_effects_skin_rash_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_skin_rash): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET side_effects_skin_rash = NULL, side_effects_skin_rash_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_skin_rash): #{patient_id}"

            end #voided

          when side_effects_lipodystrophy
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, side_effects_lipodystrophy, side_effects_lipodystrophy_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (side_effects: value_coded==side_effects_lipodystrophy): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET side_effects_lipodystrophy = 'Yes', side_effects_lipodystrophy_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_lipodystrophy): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET side_effects_lipodystrophy = NULL, side_effects_lipodystrophy_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_lipodystrophy): #{patient_id}"

            end #voided

          when side_effects_other
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, side_effects_other, side_effects_other_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (side_effects: value_coded==side_effects_other): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET side_effects_other = 'Yes', side_effects_other_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_other): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET side_effects_other = NULL, side_effects_other_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_other): #{patient_id}"

            end #voided

          when side_effects_kidney_failure
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, side_effects_kidney_failure, side_effects_kidney_failure_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (side_effects: value_coded==side_effects_kidney_failure): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET side_effects_kidney_failure = 'Yes', side_effects_kidney_failure_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_kidney_failure): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET side_effects_kidney_failure = NULL, side_effects_kidney_failure_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_kidney_failure): #{patient_id}"

            end #voided

          when side_effects_nightmares
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, side_effects_nightmares, side_effects_nightmares_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
              puts "........... Inserting record into flat_table2 (side_effects: value_coded==side_effects_nightmares): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET side_effects_nightmares = 'Yes', side_effects_nightmares_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_nightmares): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET side_effects_nightmares = NULL, side_effects_nightmares_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_nightmares): #{patient_id}"

            end #voided

          when side_effects_diziness
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, side_effects_diziness, side_effects_diziness_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (side_effects: value_coded==side_effects_diziness): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET side_effects_diziness = 'Yes', side_effects_diziness_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_diziness): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET side_effects_diziness = NULL, side_effects_diziness_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_diziness): #{patient_id}"

            end #voided

          when side_effects_psychosis
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, side_effects_psychosis, side_effects_psychosis_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (side_effects: value_coded==side_effects_psychosis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET side_effects_psychosis = 'Yes', side_effects_psychosis_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_psychosis): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET side_effects_psychosis = NULL, side_effects_psychosis_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_psychosis): #{patient_id}"

            end #voided

          when side_effects_blurry_vision
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, side_effects_blurry_vision, side_effects_blurry_vision_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (side_effects: value_coded==side_effects_blurry_vision): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET side_effects_blurry_vision = 'Yes', side_effects_blurry_vision_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_blurry_vision): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET side_effects_blurry_vision = NULL, side_effects_blurry_vision_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (side_effects: value_coded==side_effects_blurry_vision): #{patient_id}"

            end #voided
          end #case

        when routine_tb_screening
          routine_tb_screening_fever_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Fever' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          routine_tb_screening_fever = routine_tb_screening_fever_record['concept_id']
    
          routine_tb_screening_night_sweats_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Night sweats' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          routine_tb_screening_night_sweats = routine_tb_screening_night_sweats_record['concept_id']
    
          routine_tb_screening_cough_of_any_duration_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Cough of any duration' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          routine_tb_screening_cough_of_any_duration = routine_tb_screening_cough_of_any_duration_record['concept_id']
    
          routine_tb_screening_weight_loss_failure_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Weight loss / Failure to thrive / malnutrition' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")

          case value_coded
          when routine_tb_screening_fever
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, routine_tb_screening_fever, routine_tb_screening_fever_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (routine_tb_screening: value_coded==routine_tb_screening_fever): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET routine_tb_screening_fever = 'Yes', routine_tb_screening_fever_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (routine_tb_screening: value_coded==routine_tb_screening_fever): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET routine_tb_screening_fever = NULL, routine_tb_screening_fever_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (routine_tb_screening: value_coded==routine_tb_screening_fever): #{patient_id}"

            end #voided

          when routine_tb_screening_night_sweats
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, routine_tb_screening_night_sweats, routine_tb_screening_night_sweats_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (routine_tb_screening: value_coded==routine_tb_screening_night_sweats): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET routine_tb_screening_night_sweats = 'Yes', routine_tb_screening_night_sweats_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (routine_tb_screening: value_coded==routine_tb_screening_night_sweats): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET routine_tb_screening_night_sweats = NULL, routine_tb_screening_night_sweats_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (routine_tb_screening: value_coded==routine_tb_screening_night_sweats): #{patient_id}"

            end #voided

          when routine_tb_screening_cough_of_any_duration
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, routine_tb_screening_cough_of_any_duration, routine_tb_screening_cough_of_any_duration_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (routine_tb_screening: value_coded==routine_tb_screening_cough_of_any_duration): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET routine_tb_screening_cough_of_any_duration = 'Yes', routine_tb_screening_cough_of_any_duration_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (routine_tb_screening: value_coded==routine_tb_screening_cough_of_any_duration): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET routine_tb_screening_cough_of_any_duration = NULL, routine_tb_screening_cough_of_any_duration_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (routine_tb_screening: value_coded==routine_tb_screening_cough_of_any_duration): #{patient_id}"

            end #voided

          when routine_tb_screening_weight_loss_failure
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, routine_tb_screening_weight_loss_failure, routine_tb_screening_weight_loss_failure_enc_id) VALUES (in_patient_id, in_visit_date, 'Yes', encounter_id);
EOF
                puts "........... Inserting record into flat_table2 (routine_tb_screening: value_coded==routine_tb_screening_weight_loss_failure): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET routine_tb_screening_weight_loss_failure = 'Yes', routine_tb_screening_weight_loss_failure_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (routine_tb_screening: value_coded==routine_tb_screening_weight_loss_failure): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET routine_tb_screening_weight_loss_failure = NULL, routine_tb_screening_weight_loss_failure_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (routine_tb_screening: value_coded==routine_tb_screening_weight_loss_failure): #{patient_id}"

            end #voided
          end #case

        when allergic_to_sulphur
          yes_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'Yes' AND voided = 0 AND retired = 0 ")
          yes = yes_record['concept_id']

          no_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name 
                LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id 
                WHERE name = 'No' AND voided = 0 AND retired = 0 ")
          no = no_record['concept_id']

          case value_coded
          when yes
            value_record = Connection.execute("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, allergic_to_sulphur_yes, allergic_to_sulphur_yes_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (allergic_to_sulphur: value_coded==yes): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET allergic_to_sulphur_yes = "#{value}", allergic_to_sulphur_no = NULL, allergic_to_sulphur_yes_enc_id = "#{encounter_id}", allergic_to_sulphur_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (allergic_to_sulphur: value_coded==yes): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET allergic_to_sulphur_yes = NULL, allergic_to_sulphur_no = NULL, allergic_to_sulphur_yes_enc_id = NULL, allergic_to_sulphur_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (allergic_to_sulphur: value_coded==yes): #{patient_id}"

            end #voided

          when no
            value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = #{value_coded_name_id}")
            value = value_record['name']

            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, allergic_to_sulphur_no, allergic_to_sulphur_no_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (allergic_to_sulphur: value_coded==no): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET allergic_to_sulphur_no = "#{value}", allergic_to_sulphur_yes = NULL, allergic_to_sulphur_yes_enc_id = NULL, allergic_to_sulphur_no_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (allergic_to_sulphur: value_coded==no): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET allergic_to_sulphur_no = NULL, allergic_to_sulphur_yes = NULL, allergic_to_sulphur_yes_enc_id = NULL, allergic_to_sulphur_no_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (allergic_to_sulphur: value_coded==no): #{patient_id}"

            end #voided
          end #case

        when tb_status
          tb_status_tb_not_suspected_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'TB NOT suspected' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          tb_status_tb_not_suspected = tb_status_tb_not_suspected_record['concept_id']
    
          tb_status_tb_suspected_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'TB suspected' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          tb_status_tb_suspected = tb_status_tb_suspected_record['concept_id']
    
          tb_status_confirmed_tb_not_on_treatment_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Confirmed TB NOT on treatment' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          tb_status_confirmed_tb_not_on_treatment = tb_status_confirmed_tb_not_on_treatment_record['concept_id']
    
          tb_status_confirmed_tb_on_treatment_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Confirmed TB on treatment' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")
          tb_status_confirmed_tb_on_treatment = tb_status_confirmed_tb_on_treatment_record['concept_id']
    
          tb_status_unknown_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE name = 'Unknown' AND voided = 0 AND retired = 0 ORDER BY concept_name.concept_id DESC ")

          case value_coded
          when tb_status_tb_not_suspected
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, tb_status_tb_not_suspected, tb_status_tb_not_suspected_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (tb_status: value_coded==tb_status_tb_not_suspected): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET tb_status_tb_not_suspected = 'Yes', tb_status_tb_not_suspected_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (tb_status: value_coded==tb_status_tb_not_suspected): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET tb_status_tb_not_suspected = NULL, tb_status_tb_not_suspected_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (tb_status: value_coded==tb_status_tb_not_suspected): #{patient_id}"

            end #voided

          when tb_status_tb_suspected
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, tb_status_tb_suspected, tb_status_tb_suspected_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (tb_status: value_coded==tb_status_tb_suspected): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET tb_status_tb_suspected = 'Yes', tb_status_tb_suspected_enc_id = encounter_id WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (tb_status: value_coded==tb_status_tb_suspected): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET tb_status_tb_suspected = NULL, tb_status_tb_suspected_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (tb_status: value_coded==tb_status_tb_suspected): #{patient_id}"

            end #voided

          when tb_status_confirmed_tb_not_on_treatment
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, tb_status_confirmed_tb_not_on_treatment, tb_status_confirmed_tb_not_on_treatment_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (tb_status: value_coded==tb_status_confirmed_tb_not_on_treatment): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET tb_status_confirmed_tb_not_on_treatment = 'Yes', tb_status_confirmed_tb_not_on_treatment_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (tb_status: value_coded==tb_status_confirmed_tb_not_on_treatment): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET tb_status_confirmed_tb_not_on_treatment = NULL, tb_status_confirmed_tb_not_on_treatment_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (tb_status: value_coded==tb_status_confirmed_tb_not_on_treatment): #{patient_id}"

            end #voided

          when tb_status_confirmed_tb_on_treatment
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, tb_status_confirmed_tb_on_treatment, tb_status_confirmed_tb_on_treatment_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (tb_status: value_coded==tb_status_confirmed_tb_on_treatment): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET tb_status_confirmed_tb_on_treatment = 'Yes', tb_status_confirmed_tb_on_treatment_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (tb_status: value_coded==tb_status_confirmed_tb_on_treatment): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET tb_status_confirmed_tb_on_treatment = NULL, tb_status_confirmed_tb_on_treatment_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
              puts "........... Updating record into flat_table2 (tb_status: value_coded==tb_status_confirmed_tb_on_treatment): #{patient_id}"

            end #voided

          when tb_status_unknown
            if voided.blank?
              if visit.blank?
                Connection.execute <<EOF
                INSERT INTO flat_table2 (patient_id, visit_date, tb_status_unknown, tb_status_unknown_enc_id) VALUES ("#{patient_id}", '#{Current_date}', 'Yes', "#{encounter_id}");
EOF
                puts "........... Inserting record into flat_table2 (tb_status: value_coded==tb_status_unknown): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET tb_status_unknown = 'Yes', tb_status_unknown_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                puts "........... Updating record into flat_table2 (tb_status: value_coded==tb_status_unknown): #{patient_id}"

              end #visit

            else
              Connection.execute <<EOF
              UPDATE flat_table2 SET tb_status_unknown = NULL, tb_status_unknown_enc_id = NULL WHERE flat_table2.id = "#{in_visit_id}";
EOF
              puts "........... Updating record into flat_table2 (tb_status: value_coded==tb_status_unknown): #{patient_id}"

            end #voided
          end #case

        when reason_for_eligibility
          if voided.blank?
            answer_record = Connection.select_one("SELECT concept_name.name FROM concept_name
                          LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                          WHERE concept.concept_id = '#{value_coded}' AND name <> ' ' AND voided = 0 AND retired = 0 ")
            answer = answer_record['name']

            Connection.execute <<EOF
            UPDATE flat_table1 SET reason_for_eligibility = "#{answer}", reason_for_starting_v_date = '#{Current_date}', reason_for_eligibility_enc_id = "#{encounter_id}" WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
            puts "........... Updating record into flat_table1 (reason_for_eligibility): #{patient_id}"

          else
            Connection.execute <<EOF
            UPDATE flat_table1 SET reason_for_eligibility = NULL, reason_for_starting_v_date = NULL, reason_for_eligibility_enc_id = NULL WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
            puts "........... Updating record into flat_table1 (reason_for_eligibility): #{patient_id}"

          end #voided

        when who_stage
          if voided.blank?
            stage_record = Connection.select_one("SELECT concept_name.name FROM concept_name
                          LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                          WHERE concept.concept_id = '#{value_coded}' AND name <> ' ' AND voided = 0 AND retired = 0 ")
            stage = stage_record['name']

            Connection.execute <<EOF
            UPDATE flat_table1 SET who_stage = "#{stage}" WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
            puts "........... Updating record into flat_table1 (who_stage): #{patient_id}"

          else
            Connection.execute <<EOF
            UPDATE flat_table1 SET who_stage = NULL WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
            puts "........... Updating record into flat_table1 (who_stage): #{patient_id}"

          end #voided

        when send_sms
          if voided.blank?
            answer_record = Connection.select_one("SELECT concept_name.name FROM concept_name
                          LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                          WHERE concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
            answer = answer_record['name']

            Connection.execute <<EOF
                    UPDATE flat_table1 SET send_sms = "#{answer}" WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
            puts "........... Updating record into flat_table1 (send_sms): #{patient_id}"

          else
            Connection.execute <<EOF
            UPDATE flat_table1 SET send_sms = NULL WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
            puts "........... Updating record into flat_table1 (send_sms): #{patient_id}"

          end #voided


        when agrees_to_followup
          if voided.blank?
            answer_record = Connection.select_one("SELECT concept_name.name FROM concept_name
                          LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                          WHERE concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
            answer = answer_record['name']

            Connection.execute <<EOF
            UPDATE flat_table1 SET agrees_to_followup = "#{answer}" WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
            puts "........... Updating record into flat_table1 (agrees_to_followup): #{patient_id}"

          else
            Connection.execute <<EOF
            UPDATE flat_table1 SET agrees_to_followup = NULL WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
            puts "........... Updating record into flat_table1 (agrees_to_followup): #{patient_id}"

          end #voided


        when type_of_confirmatory_hiv_test
          if voided.blank?
            answer_record = Connection.select_one("SELECT concept_name.name FROM concept_name
                          LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                          WHERE concept.concept_id = '#{field_value_coded}' AND voided = 0 AND retired = 0 ")
            answer = answer_record['name']

            Connection.execute <<EOF
            UPDATE flat_table1 SET type_of_confirmatory_hiv_test = "#{answer}" WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
            puts "........... Updating record into flat_table1 (type_of_confirmatory_hiv_test): #{patient_id}"

          else
            Connection.execute <<EOF
            UPDATE flat_table1 SET type_of_confirmatory_hiv_test = NULL WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
            puts "........... Updating record into flat_table1 (type_of_confirmatory_hiv_test): #{patient_id}"

          end #voided

        when confirmatory_hiv_test_location
          if voided.blank?
            answer_record = Connection.select_one("SELECT name FROM location WHERE location_id = '#{value_text}' ")
            answer = answer_record['name']

            if value_text.present?
              Connection.execute <<EOF
              UPDATE flat_table1 SET confirmatory_hiv_test_location = "#{value_text}" WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
              puts "........... Updating record into flat_table1 (confirmatory_hiv_test_location): #{patient_id}"

            else
              if answer.blank?
                Connection.execute <<EOF
                UPDATE flat_table1 SET confirmatory_hiv_test_location = "Unknown" WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
                puts "........... Updating record into flat_table1 (confirmatory_hiv_test_location): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET confirmatory_hiv_test_location = "#{answer}" WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
                puts "........... Updating record into flat_table1 (confirmatory_hiv_test_location): #{patient_id}"

              end #answer
            end #value_text
          
          else
            Connection.execute <<EOF
            UPDATE flat_table1 SET confirmatory_hiv_test_location = NULL WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
            puts "........... Updating record into flat_table1 (confirmatory_hiv_test_location): #{patient_id}"

          end #voided

        when cd4_count
          if voided.blank?
            Connection.execute <<EOF
            UPDATE flat_table1 SET cd4_count = "#{value_numeric}" WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
            Connection.execute <<EOF
            UPDATE flat_table1 SET cd4_count_modifier = "#{value_modifier}" WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
            puts "........... Updating record into flat_table1 (cd4_count): #{patient_id}"

          else
            Connection.execute <<EOF
            UPDATE flat_table1 SET cd4_count = NULL WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
            Connection.execute <<EOF
            UPDATE flat_table1 SET cd4_count_modifier = NULL WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
            puts "........... Updating record into flat_table1 (cd4_count): #{patient_id}"

          end #voided


        else

          weight_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'Weight' AND voided = 0 AND retired = 0 ")
          weight = weight_record['concept_id']

          height_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'Height (cm)' AND voided = 0 AND retired = 0 ")
          height = height_record['concept_id']

          temperature_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'Temperature' AND voided = 0 AND retired = 0 ")
          temperature = temperature_record['concept_id']

          bmi_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'BMI' AND voided = 0 AND retired = 0 ")
          bmi = bmi_record['concept_id']

          systolic_blood_pressure_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'Systolic blood pressure' AND voided = 0 AND retired = 0 ")
          systolic_blood_pressure = systolic_blood_pressure_record['concept_id']

          diastolic_blood_pressure_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'Diastolic blood pressure' AND voided = 0 AND retired = 0 ")
          diastolic_blood_pressure = diastolic_blood_pressure_record['concept_id']

          weight_for_height_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'Weight for height percent of median' AND voided = 0 AND retired = 0 ")
          weight_for_height = weight_for_height_record['concept_id']

          weight_for_age_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'Weight for age percent of median' AND voided = 0 AND retired = 0 ")
          weight_for_age = weight_for_age_record['concept_id']

          height_for_age_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'Height for age percent of median' AND voided = 0 AND retired = 0 ")
          height_for_age = height_for_age_record['concept_id']

          regimen_category_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'Regimen Category' AND voided = 0 AND retired = 0 ")
          regimen_category = regimen_category_record['concept_id']

          transfer_out_location_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'Transfer out to' AND voided = 0 AND retired = 0 ")
          transfer_out_location = transfer_out_location_record['concept_id']

          appointment_date_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'Appointment date' AND voided = 0 AND retired = 0 ")
          appointment_date = appointment_date_record['concept_id']

          condoms_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'Condoms' AND voided = 0 AND retired = 0 ")
          condoms = condoms_record['concept_id']

          cpt_given_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'CPT started' AND voided = 0 AND retired = 0 ")
          cpt_given = cpt_given_record['concept_id']

          ipt_given_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'Isoniazid' AND voided = 0 AND retired = 0 ")
          ipt_given = ipt_given_record['concept_id']

          amount_of_drug_brought_to_clinic_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'Amount of drug brought to clinic' AND voided = 0 AND retired = 0 ")
          amount_of_drug_brought_to_clinic = amount_of_drug_brought_to_clinic_record['concept_id']

          amount_of_drug_remaining_at_home_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'Amount of drug remaining at home' AND voided = 0 AND retired = 0 ")
          amount_of_drug_remaining_at_home = amount_of_drug_remaining_at_home_record['concept_id']

          what_was_the_patient_adherence_for_this_drug_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'What was the patients adherence for this drug order' AND voided = 0 AND retired = 0 ")
          what_was_the_patient_adherence_for_this_drug = what_was_the_patient_adherence_for_this_drug_record['concept_id']

          missed_hiv_drug_construct_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name
            LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
            WHERE name = 'Missed HIV drug construct' AND voided = 0 AND retired = 0 ")
          missed_hiv_drug_construct = missed_hiv_drug_construct_record['concept_id']

          malawi_ART_side_effects_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'malawi ART side effects' AND voided = 0 AND retired = 0 ")
          malawi_ART_side_effects = malawi_ART_side_effects_record['concept_id']

          ever_received_art_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'ever received art' AND voided = 0 AND retired = 0 ")
          ever_received_art = ever_received_art_record['concept_id']

          date_last_taken_arv_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Date ART last taken' AND voided = 0 AND retired = 0 ")
          date_last_taken_arv = date_last_taken_arv_record['concept_id']

          art_in_2_months_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Has the patient taken ART in the last two months' AND voided = 0 AND retired = 0 ")
          art_in_2_months = art_in_2_months_record['concept_id']

          art_in_2_weeks_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Has the patient taken ART in the last two weeks' AND voided = 0 AND retired = 0 ")
          art_in_2_weeks = art_in_2_weeks_record['concept_id']

          last_arv_reg_record =  Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Last ART drugs taken' AND voided = 0 AND retired = 0 ")
          last_arv_reg = last_arv_reg_record['concept_id']

          ever_reg_4_art_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Ever registered at ART clinic' AND voided = 0 AND retired = 0 ")
          ever_reg_4_art = ever_reg_4_art_record['concept_id']

          has_transfer_letter_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Has transfer letter' AND voided = 0 AND retired = 0 ")
          has_transfer_letter = has_transfer_letter_record['concept_id']

          art_init_loc_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Location of ART INITIATION' AND voided = 0 AND retired = 0 ")
          art_init_loc = art_init_loc_record['concept_id']

          art_start_date_est_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Has transfer letter' AND voided = 0 AND retired = 0 ")
          art_start_date_est = art_start_date_est_record['concept_id']

          date_started_art_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'ART start date' AND voided = 0 AND retired = 0 ")
          date_started_art = date_started_art_record['concept_id']

          cd4_count_loc_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Cd4 count location' AND voided = 0 AND retired = 0 ")
          cd4_count_loc = cd4_count_loc_record['concept_id']

          cd4_percent_loc_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'CD4 percent location' AND voided = 0 AND retired = 0 ")
          cd4_percent_loc = cd4_percent_loc_record['concept_id']

          cd4_count_date_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Cd4 count datetime' AND voided = 0 AND retired = 0 ")

          cd4_count_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Cd4 count' AND voided = 0 AND retired = 0 ")
          cd4_count = cd4_count_record['concept_id']

          cd4_count_percent_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Cd4 percent' AND voided = 0 AND retired = 0 ")

          cd4_count_mod_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Cd4 count' AND voided = 0 AND retired = 0 ")
          cd4_count_mod = cd4_count_mod_record['concept_id']

          cd4_percent_less_than_25_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'CD4 percent less than 25' AND voided = 0 AND retired = 0 ")
          cd4_percent_less_than_25 = cd4_percent_less_than_25_record['concept_id']

          cd4_count_less_than_250_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'CD4 count less than 250' AND voided = 0 AND retired = 0 ")
          cd4_count_less_than_250 = cd4_count_less_than_250_record['concept_id']

          cd4_count_less_than_350_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'CD4 count less than or equal to 350' AND voided = 0 AND retired = 0 ")
          cd4_count_less_than_350 = cd4_count_less_than_350_record['concept_id']

          pnuemocystis_pnuemonia_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Pneumocystis pneumonia' AND voided = 0 AND retired = 0 ")
          pnuemocystis_pnuemonia = pnuemocystis_pnuemonia_record['concept_id']

          lymphocyte_count_date_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Lymphocyte count datetime' AND voided = 0 AND retired = 0 ")
          lymphocyte_count_date = lymphocyte_count_date_record['concept_id']

          lymphocyte_count_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Lymphocyte count' AND voided = 0 AND retired = 0 ")
          lymphocyte_count = lymphocyte_count_record['concept_id']

          asymptomatic_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Asymptomatic HIV infection' AND voided = 0 AND retired = 0 ")

          pers_gnrl_lymphadenopathy_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Persistent generalized lymphadenopathy' AND voided = 0 AND retired = 0 ")
          pers_gnrl_lymphadenopathy = pers_gnrl_lymphadenopathy_record['concept_id']

          unspecified_stage_1_cond_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Unspecified stage I condition' AND voided = 0 AND retired = 0 ")
          unspecified_stage_1_cond = unspecified_stage_1_cond_record['concept_id']

          molluscumm_contagiosum_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Molluscum contagiosum' AND voided = 0 AND retired = 0 ")
          molluscumm_contagiosum = molluscumm_contagiosum_record['concept_id']

          wart_virus_infection_extensive_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Wart virus infection, extensive' AND voided = 0 AND retired = 0 ")
          wart_virus_infection_extensive = wart_virus_infection_extensive_record['concept_id']

          oral_ulcerations_recurrent_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Oral ulcerations, recurrent' AND voided = 0 AND retired = 0 ")
          oral_ulcerations_recurrent = oral_ulcerations_recurrent_record['concept_id']

          parotid_enlargement_pers_unexp_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Parotid enlargement' AND voided = 0 AND retired = 0 ")
          parotid_enlargement_pers_unexp = parotid_enlargement_pers_unexp_record['concept_id']

          lineal_gingival_erythema_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Lineal gingival erythema' AND voided = 0 AND retired = 0 ")
          lineal_gingival_erythema = lineal_gingival_erythema_record['concept_id']

          herpes_zoster_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Herpes zoster' AND voided = 0 AND retired = 0 ")
          herpes_zoster = herpes_zoster_record['concept_id']

          resp_tract_infections_rec_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Respiratory tract infections, recurrent (sinusitis, tonsilitus, otitis media, pharyngitis)' AND voided = 0 AND retired = 0 ")
          resp_tract_infections_rec = resp_tract_infections_rec_record['concept_id']

          unspecified_stage2_condition_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Unspecified stage II condition' AND voided = 0 AND retired = 0 ")
          unspecified_stage2_condition = unspecified_stage2_condition_record['concept_id']

          angular_chelitis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Angular cheilitis' AND voided = 0 AND retired = 0 ")
          angular_chelitis = angular_chelitis_record['concept_id']

          papular_prurtic_eruptions_record = Connection.select_one("SELECT concept_name.concept_id FROM  concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Papular pruritic eruptions / Fungal nail infections' AND voided = 0 AND retired = 0 ")
          papular_prurtic_eruptions = papular_prurtic_eruptions_record['concept_id']

          hepatosplenomegaly_unexplained_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Hepatosplenomegaly persistent unexplained' AND voided = 0 AND retired = 0 ")
          hepatosplenomegaly_unexplained = hepatosplenomegaly_unexplained_record['concept_id']

          oral_hairy_leukoplakia_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Oral hairy leukoplakia' AND voided = 0 AND retired = 0 ")
          oral_hairy_leukoplakia = oral_hairy_leukoplakia_record['concept_id']

          severe_weight_loss_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Severe weight loss >10% and/or BMI <18.5kg/m^2, unexplained' AND voided = 0 AND retired = 0 ")
          severe_weight_loss = severe_weight_loss_record['concept_id']

          fever_persistent_unexplained_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Fever, persistent unexplained, intermittent or constant, >1 month' AND voided = 0 AND retired = 0 ")
          fever_persistent_unexplained = fever_persistent_unexplained_record['concept_id']

          pulmonary_tuberculosis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Pulmonary tuberculosis (current)' AND voided = 0 AND retired = 0 ")
          pulmonary_tuberculosis = pulmonary_tuberculosis_record['concept_id']

          pulmonary_tuberculosis_last_2_years_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Tuberculosis (PTB or EPTB) within the last 2 years' AND voided = 0 AND retired = 0 ")
          pulmonary_tuberculosis_last_2_years = pulmonary_tuberculosis_last_2_years_record['concept_id']

          severe_bacterial_infection_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Severe bacterial infections (pneumonia, empyema, pyomyositis, bone/joint, meningitis, bacteraemia)' AND voided = 0 AND retired = 0 ")
          severe_bacterial_infection = severe_bacterial_infection_record['concept_id']

          bacterial_pnuemonia_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Bacterial pneumonia, severe recurrent' AND voided = 0 AND retired = 0 ")
          bacterial_pnuemonia = bacterial_pnuemonia_record['concept_id']

          symptomatic_lymphoid_interstitial_pnuemonitis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Symptomatic lymphoid interstitial pneumonia' AND voided = 0 AND retired = 0 ")
          symptomatic_lymphoid_interstitial_pnuemonitis = symptomatic_lymphoid_interstitial_pnuemonitis_record['concept_id']

          chronic_hiv_assoc_lung_disease_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Chronic HIV lung disease' AND voided = 0 AND retired = 0 ")
          chronic_hiv_assoc_lung_disease = chronic_hiv_assoc_lung_disease_record['concept_id']

          unspecified_stage3_condition_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Unspecified stage III condition' AND voided = 0 AND retired = 0 ")
          unspecified_stage3_condition = unspecified_stage3_condition_record['concept_id']

          aneamia_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Anaemia, unexplained < 8 g/dl' AND voided = 0 AND retired = 0 ")
          aneamia = aneamia_record['concept_id']

          neutropaenia_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Neutropaenia, unexplained < 500 /mm(cubed)' AND voided = 0 AND retired = 0 ")
          neutropaenia = neutropaenia_record['concept_id']

          thrombocytopaenia_chronic_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Thrombocytopaenia, chronic < 50,000 /mm(cubed)' AND voided = 0 AND retired = 0 ")
          thrombocytopaenia_chronic = thrombocytopaenia_chronic_record['concept_id']

          diarhoea_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Diarrhoea, chronic (>1 month) unexplained' AND voided = 0 AND retired = 0 ")
          diarhoea = diarhoea_record['concept_id']

          oral_candidiasis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Oral candidiasis' AND voided = 0 AND retired = 0 ")
          oral_candidiasis = oral_candidiasis_record['concept_id']

          acute_necrotizing_ulcerative_gingivitis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name LIKE '%Acute necrotizing ulcerative stomatitis, gingivitis or periodontitis%' AND voided = 0 AND retired = 0 ")
          acute_necrotizing_ulcerative_gingivitis = acute_necrotizing_ulcerative_gingivitis_record['concept_id']


          lymph_node_tuberculosis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Lymph node tuberculosis' AND voided = 0 AND retired = 0 ")
          lymph_node_tuberculosis = lymph_node_tuberculosis_record['concept_id']

          toxoplasmosis_of_brain_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Toxoplasmosis of the brain' AND voided = 0 AND retired = 0 ")
          toxoplasmosis_of_brain = toxoplasmosis_of_brain_record['concept_id']

          cryptococcal_meningitis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Cryptococcal meningitis or other extrapulmonary cryptococcosis' AND voided = 0 AND retired = 0 ")
          cryptococcal_meningitis = cryptococcal_meningitis_record['concept_id']

          progressive_multifocal_leukoencephalopathy_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Progressive multifocal leukoencephalopathy' AND voided = 0 AND retired = 0 ")
          progressive_multifocal_leukoencephalopathy = progressive_multifocal_leukoencephalopathy_record['concept_id']

          disseminated_mycosis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Disseminated mycosis (coccidiomycosis or histoplasmosis)' AND voided = 0 AND retired = 0 ")
          disseminated_mycosis = disseminated_mycosis_record['concept_id']

          candidiasis_of_oesophagus_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Candidiasis of oseophagus, trachea and bronchi or lungs' AND voided = 0 AND retired = 0 ")
          candidiasis_of_oesophagus = candidiasis_of_oesophagus_record['concept_id']

          extrapulmonary_tuberculosis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Extrapulmonary tuberculosis (EPTB)' AND voided = 0 AND retired = 0 ")
          extrapulmonary_tuberculosis = extrapulmonary_tuberculosis_record['concept_id']

          cerebral_non_hodgkin_lymphoma_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Cerebral or B-cell non Hodgkin lymphoma' AND voided = 0 AND retired = 0 ")
          cerebral_non_hodgkin_lymphoma = cerebral_non_hodgkin_lymphoma_record['concept_id']


          hiv_encephalopathy_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'HIV encephalopathy' AND voided = 0 AND retired = 0 ")
          hiv_encephalopathy = hiv_encephalopathy_record['concept_id']

          bacterial_infections_severe_recurrent_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Bacterial infections, severe recurrent  (empyema, pyomyositis, meningitis, bone/joint infections but EXCLUDING pneumonia)' AND voided = 0 AND retired = 0 ")
          bacterial_infections_severe_recurrent = bacterial_infections_severe_recurrent_record['concept_id']

          unspecified_stage_4_condition_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Unspecified stage IV condition' AND voided = 0 AND retired = 0 ")
          unspecified_stage_4_condition = unspecified_stage_4_condition_record['concept_id']

          disseminated_non_tuberculosis_mycobactierial_infection_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Disseminated non-tuberculosis mycobacterial infection' AND voided = 0 AND retired = 0 ")
          disseminated_non_tuberculosis_mycobactierial_infection = disseminated_non_tuberculosis_mycobactierial_infection_record['concept_id']

          cryptosporidiosis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Cryptosporidiosis, chronic with diarroea' AND voided = 0 AND retired = 0 ")
          cryptosporidiosis = cryptosporidiosis_record['concept_id']

          isosporiasis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Isosporiasis >1 month' AND voided = 0 AND retired = 0 ")
          isosporiasis = isosporiasis_record['concept_id']

          symptomatic_hiv_asscoiated_nephropathy_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Symptomatic HIV associated nephropathy or cardiomyopathy' AND voided = 0 AND retired = 0 ")
          symptomatic_hiv_asscoiated_nephropathy = symptomatic_hiv_asscoiated_nephropathy_record['concept_id']

          chronic_herpes_simplex_infection_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Chronic herpes simplex infection (orolabial, gential / anorectal >1 month or visceral at any site)' AND voided = 0 AND retired = 0 ")
          chronic_herpes_simplex_infection = chronic_herpes_simplex_infection_record['concept_id']

          cytomegalovirus_infection_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Cytomegalovirus infection' AND voided = 0 AND retired = 0 ")
          cytomegalovirus_infection = cytomegalovirus_infection_record['concept_id']

          toxoplasomis_of_the_brain_1month_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Toxoplasmosis, brain > 1 month' AND voided = 0 AND retired = 0 ")
          toxoplasomis_of_the_brain_1month = toxoplasomis_of_the_brain_1month_record['concept_id']

          recto_vaginal_fitsula_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Rectovaginal fistula' AND voided = 0 AND retired = 0 ")
          recto_vaginal_fitsula = recto_vaginal_fitsula_record['concept_id']

          mod_wght_loss_less_thanequal_to_10_perc_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Moderate weight loss less than or equal to 10 percent, unexplained' AND voided = 0 AND retired = 0 ")
          mod_wght_loss_less_thanequal_to_10_perc = mod_wght_loss_less_thanequal_to_10_perc_record['concept_id']

          seborrhoeic_dermatitis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Seborrhoeic dermatitis' AND voided = 0 AND retired = 0 ")
          seborrhoeic_dermatitis = seborrhoeic_dermatitis_record['concept_id']

          hepatitis_b_or_c_infection_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Hepatitis B or C infection' AND voided = 0 AND retired = 0 ")
          hepatitis_b_or_c_infection = hepatitis_b_or_c_infection_record['concept_id']

          kaposis_sarcoma_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Kaposis sarcoma' AND voided = 0 AND retired = 0 ")
          kaposis_sarcoma = kaposis_sarcoma_record['concept_id']

          non_typhoidal_salmonella_bacteraemia_recurrent_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Non-typhoidal salmonella bacteraemia, recurrent' AND voided = 0 AND retired = 0 ")
          non_typhoidal_salmonella_bacteraemia_recurrent = non_typhoidal_salmonella_bacteraemia_recurrent_record['concept_id']

          leishmaniasis_atypical_disseminated_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Atypical disseminated leishmaniasis' AND voided = 0 AND retired = 0 ")
          leishmaniasis_atypical_disseminated = leishmaniasis_atypical_disseminated_record['concept_id']

          cerebral_or_b_cell_non_hodgkin_lymphoma_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Cerebral or B-cell non Hodgkin lymphoma' AND voided = 0 AND retired = 0 ")
          cerebral_or_b_cell_non_hodgkin_lymphoma = cerebral_or_b_cell_non_hodgkin_lymphoma_record['concept_id']

          invasive_cancer_of_cervix_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'invasive cancer of cervix' AND voided = 0 AND retired = 0 ")
          invasive_cancer_of_cervix = invasive_cancer_of_cervix_record['concept_id']

          cryptococcal_meningitis_or_other_eptb_cryptococcosis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Cryptococcal meningitis or other extrapulmonary cryptococcosis' AND voided = 0 AND retired = 0 ")
          cryptococcal_meningitis_or_other_eptb_cryptococcosis = cryptococcal_meningitis_or_other_eptb_cryptococcosis_record['concept_id']

          candidiasis_of_oesophagus_trachea_bronchi_or_lungs_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Candidiasis of oseophagus, trachea and bronchi or lungs' AND voided = 0 AND retired = 0 ")
          candidiasis_of_oesophagus_trachea_bronchi_or_lungs = candidiasis_of_oesophagus_trachea_bronchi_or_lungs_record['concept_id']

          severe_unexplained_wasting_malnutrition_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Severe unexplained wasting or malnutrition not responding to treatment (weight-for-height/ -age <70% or MUAC less than 11cm or oedema)' AND voided = 0 AND retired = 0 ")
          severe_unexplained_wasting_malnutrition = severe_unexplained_wasting_malnutrition_record['concept_id']

          diarrhoea_chronic_less_1_month_unexplained_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Diarrhoea, chronic (>1 month) unexplained' AND voided = 0 AND retired = 0 ")
          diarrhoea_chronic_less_1_month_unexplained = diarrhoea_chronic_less_1_month_unexplained_record['concept_id']

          moderate_weight_loss_10_unexplained_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Moderate weight loss less than or equal to 10 percent, unexplained' AND voided = 0 AND retired = 0 ")
          moderate_weight_loss_10_unexplained = moderate_weight_loss_10_unexplained_record['concept_id']

=begin
  
cd4_percentage_available_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'CD4 percent available' AND voided = 0 AND retired = 0 ")
          cd4_percentage_available = cd4_percentage_available_record['concept_id']

          acute_necrotizing_ulcerative_stomatitis_gingivitis_or_period_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Acute necrotizing ulcerative stomatitis, gingivitis or periodontitis' AND voided = 0 AND retired = 0 ")
          acute_necrotizing_ulcerative_stomatitis_gingivitis_or_period = acute_necrotizing_ulcerative_stomatitis_gingivitis_or_period_record['concept_id']  
=end
          

          moderate_unexplained_wasting_malnutrition_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Moderate unexplained wasting/malnutrition not responding to treatment (weight-for-height/ -age 70-79% or muac 11-12 cm)' AND voided = 0 AND retired = 0 ")
          moderate_unexplained_wasting_malnutrition = moderate_unexplained_wasting_malnutrition_record['concept_id']

          diarrhoea_persistent_unexplained_14_days_or_more_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Diarrhoea, persistent unexplained (14 days or more)' AND voided = 0 AND retired = 0 ")
          diarrhoea_persistent_unexplained_14_days_or_more = diarrhoea_persistent_unexplained_14_days_or_more_record['concept_id']

          acute_ulcerative_mouth_infections_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Acute ulcerative mouth infections' AND voided = 0 AND retired = 0 ")
          acute_ulcerative_mouth_infections = acute_ulcerative_mouth_infections_record['concept_id']

          anaemia_unexplained_8_g_dl_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Anaemia, unexplained < 8 g/dl' AND voided = 0 AND retired = 0 ")
          anaemia_unexplained_8_g_dl = anaemia_unexplained_8_g_dl_record['concept_id']

          atypical_mycobacteriosis_disseminated_or_lung_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Atypical mycobacteriosis, disseminated or lung' AND voided = 0 AND retired = 0 ")
          atypical_mycobacteriosis_disseminated_or_lung = atypical_mycobacteriosis_disseminated_or_lung_record['concept_id']

          bacterial_infections_sev_recurrent_excluding_pneumonia_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Bacterial infections, severe recurrent  (empyema, pyomyositis, meningitis, bone/joint infections but EXCLUDING pneumonia)' AND voided = 0 AND retired = 0 ")
          bacterial_infections_sev_recurrent_excluding_pneumonia = bacterial_infections_sev_recurrent_excluding_pneumonia_record['concept_id']

          cancer_cervix_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Cancer cervix' AND voided = 0 AND retired = 0 ")
          cancer_cervix = cancer_cervix_record['concept_id']

          chronic_herpes_simplex_infection_genital_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Chronic herpes simplex infection(orolabial, genital / anorectal >1 month or visceral at any site)' AND voided = 0 AND retired = 0 ")
          chronic_herpes_simplex_infection_genital = chronic_herpes_simplex_infection_genital_record['concept_id']

          cryptosporidiosis_chronic_with_diarrhoea_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Cryptosporidiosis, chronic with diarroea' AND voided = 0 AND retired = 0 ")
          cryptosporidiosis_chronic_with_diarrhoea = cryptosporidiosis_chronic_with_diarrhoea_record['concept_id']

          cytomegalovirus_infection_retinitis_or_other_organ_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Cytomegalovirus infection: rentinitis or other organ (from age 1 month)' AND voided = 0 AND retired = 0 ")
          cytomegalovirus_infection_retinitis_or_other_organ = cytomegalovirus_infection_retinitis_or_other_organ_record['concept_id']

          cytomegalovirus_of_an_organ_other_than_liver_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Cytomegalovirus of an organ other than liver, spleen or lymph node' AND voided = 0 AND retired = 0 ")
          cytomegalovirus_of_an_organ_other_than_liver = cytomegalovirus_of_an_organ_other_than_liver_record['concept_id']

          fungal_nail_infections_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Fungal nail infection' AND voided = 0 AND retired = 0 ")
          fungal_nail_infections = fungal_nail_infections_record['concept_id']

          herpes_simplex_infection_mucocutaneous_visceral_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Herpes simplex infection, mucocutaneous for longer than 1 month or visceral' AND voided = 0 AND retired = 0 ")
          herpes_simplex_infection_mucocutaneous_visceral = herpes_simplex_infection_mucocutaneous_visceral_record['concept_id']

          hiv_associated_cardiomyopathy_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'HIV associated cardiomyopathy' AND voided = 0 AND retired = 0 ")
          hiv_associated_cardiomyopathy = hiv_associated_cardiomyopathy_record['concept_id']

          hiv_associated_nephropathy_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'HIV associated nephropathy' AND voided = 0 AND retired = 0 ")
          hiv_associated_nephropathy = hiv_associated_nephropathy_record['concept_id']

          invasive_cancer_cervix_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Invasive cancer of cervix' AND voided = 0 AND retired = 0 ")
          invasive_cancer_cervix = invasive_cancer_cervix_record['concept_id']

          isosporiasis_1_month_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Isosporiasis >1 month' AND voided = 0 AND retired = 0 ")
          isosporiasis_1_month = isosporiasis_1_month_record['concept_id']

          leishmaniasis_atypical_disseminated_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Leishmaniasis, atypical disseminated' AND voided = 0 AND retired = 0 ")

          minor_mucocutaneous_manifestations_seborrheic_dermatitis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Minor mucocutaneous manifestations (seborrheic dermatitis, prurigo, fungal nail infections, recurrent oral ulcerations, angular chelitis)' AND voided = 0 AND retired = 0 ")
          minor_mucocutaneous_manifestations_seborrheic_dermatitis = minor_mucocutaneous_manifestations_seborrheic_dermatitis_record['concept_id']

          moderate_unexplained_malnutrition_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Moderate unexplained wasting/malnutrition not responding to treatment (weight-for-height/ -age 70-79% or muac 11-12 cm)' AND voided = 0 AND retired = 0 ")
          moderate_unexplained_malnutrition = moderate_unexplained_malnutrition_record['concept_id']

          molluscum_contagiosum_extensive_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Molluscum contagiosum, extensive' AND voided = 0 AND retired = 0 ")
          molluscum_contagiosum_extensive = molluscum_contagiosum_extensive_record['concept_id']

          non_typhoidal_salmonella_bacteraemia_recurrent_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Non-typhoidal Salmonella bacteraemia, recurrent' AND voided = 0 AND retired = 0 ")
          non_typhoidal_salmonella_bacteraemia_recurrent = non_typhoidal_salmonella_bacteraemia_recurrent_record['concept_id']

          oral_candidiasis_from_age_2_months_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Oral candidiasis (from age 2 months)' AND voided = 0 AND retired = 0 ")
          oral_candidiasis_from_age_2_months = oral_candidiasis_from_age_2_months_record['concept_id']

          oral_thrush_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Oral thrush' AND voided = 0 AND retired = 0 ")
          oral_thrush = oral_thrush_record['concept_id']

          perform_extended_staging_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Perform extended staging' AND voided = 0 AND retired = 0 ")
          perform_extended_staging = perform_extended_staging_record['concept_id']

          pneumocystis_carinii_pneumonia_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Pneumocystis carinii pneumonia' AND voided = 0 AND retired = 0 ")
          pneumocystis_carinii_pneumonia = pneumocystis_carinii_pneumonia_record['concept_id']

          pneumonia_severe_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Pneumonia, severe' AND voided = 0 AND retired = 0 ")
          pneumonia_severe = pneumonia_severe_record['concept_id']

          recurrent_bacteraemia_or_sepsis_with_nts_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Recurrent bacteraemia or sepsis with NTS' AND voided = 0 AND retired = 0 ")
          recurrent_bacteraemia_or_sepsis_with_nts = recurrent_bacteraemia_or_sepsis_with_nts_record['concept_id']

          recurrent_severe_presumed_pneumonia_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Recurrent severe presumed pneumonia' AND voided = 0 AND retired = 0 ")
          recurrent_severe_presumed_pneumonia = recurrent_severe_presumed_pneumonia_record['concept_id']

          recurrent_upper_respiratory_tract_bac_sinusitis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Recurrent upper respiratory infection (ie, bacterial sinusitis)' AND voided = 0 AND retired = 0 ")
          recurrent_upper_respiratory_tract_bac_sinusitis = recurrent_upper_respiratory_tract_bac_sinusitis_record['concept_id']

          seborrhoeic_dermatitis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Seborrhoeic dermatitis' AND voided = 0 AND retired = 0 ")
          seborrhoeic_dermatitis = seborrhoeic_dermatitis_record['concept_id']

          sepsis_severe_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Sepsis, severe' AND voided = 0 AND retired = 0 ")
          sepsis_severe = sepsis_severe_record['concept_id']

          tb_lymphadenopathy_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'TB lymphadenopathy' AND voided = 0 AND retired = 0 ")
          tb_lymphadenopathy = tb_lymphadenopathy_record['concept_id']

          unexplained_anaemia_neutropenia_or_thrombocytopenia_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Unexplained anaemia, neutropaenia, or throbocytopaenia' AND voided = 0 AND retired = 0 ")
          unexplained_anaemia_neutropenia_or_thrombocytopenia = unexplained_anaemia_neutropenia_or_thrombocytopenia_record['concept_id']

          visceral_leishmaniasis_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                        LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE name = 'Visceral leishmaniasis' AND voided = 0 AND retired = 0 ")
          visceral_leishmaniasis = visceral_leishmaniasis_record['concept_id']

          who_crit_stage_record = Connection.select_one("SELECT concept_name.concept_id FROM concept_name concept_name
                    LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                    WHERE name = 'Who stages criteria present' AND voided = 0 AND retired = 0 ")
          who_crit_stage = who_crit_stage_record['concept_id']
          
          if voided.blank?
            case concept_id
            when missed_hiv_drug_construct

              flat_table2_record = Connection.select_one("SELECT * FROM flat_table2 WHERE ID = '#{visit}'")

              missed_hiv_drug_construct1 = flat_table2_record['missed_hiv_drug_construct1'] rescue nil

              missed_hiv_drug_construct2 = flat_table2_record['missed_hiv_drug_construct2'] rescue nil

              missed_hiv_drug_construct3 = flat_table2_record['missed_hiv_drug_construct3'] rescue nil

              missed_hiv_drug_construct4 = flat_table2_record['missed_hiv_drug_construct4'] rescue nil

              missed_hiv_drug_construct5 = flat_table2_record['missed_hiv_drug_construct5'] rescue nil

              if visit.blank?
                case 
                when missed_hiv_drug_construct1.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, missed_hiv_drug_construct1, missed_hiv_drug_construct1_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_text}", "#{encounter_id}");
EOF
                  puts "........... Inserting record into flat_table2 (missed_hiv_drug_construct1): #{patient_id}"

                when missed_hiv_drug_construct2.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, missed_hiv_drug_construct2, missed_hiv_drug_construct2_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "value_text", "#{encounter_id}");
EOF
                  puts "........... Inserting record into flat_table2 (missed_hiv_drug_construct2): #{patient_id}"

                when missed_hiv_drug_construct3.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, missed_hiv_drug_construct3, missed_hiv_drug_construct3_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_text}", "#{encounter_id}");
EOF
                  puts "........... Inserting record into flat_table2 (missed_hiv_drug_construct3): #{patient_id}"

                when missed_hiv_drug_construct4.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, missed_hiv_drug_construct4, missed_hiv_drug_construct4_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_text}", "#{encounter_id}");
EOF
                  puts "........... Inserting record into flat_table2 (missed_hiv_drug_construct4): #{patient_id}"

                when missed_hiv_drug_construct5.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, missed_hiv_drug_construct5, missed_hiv_drug_construct5_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_text}", "#{encounter_id}");
EOF
                  puts "........... Inserting record into flat_table2 (missed_hiv_drug_construct5): #{patient_id}"

                end #case

              else

                case 
                when missed_hiv_drug_construct1.blank?
                  if voided.blank?
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET missed_hiv_drug_construct1 = "#{value_text}", missed_hiv_drug_construct1_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                    puts "........... Updating record into flat_table2 (missed_hiv_drug_construct1): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET missed_hiv_drug_construct1 = NULL, missed_hiv_drug_construct1_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                    puts "........... Updating record into flat_table2 (missed_hiv_drug_construct1): #{patient_id}"

                  end #voided

                when missed_hiv_drug_construct2.blank?
                  if voided.blank?
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET missed_hiv_drug_construct2 = "#{value_text}", missed_hiv_drug_construct2_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                    puts "........... Updating record into flat_table2 (missed_hiv_drug_construct2): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET missed_hiv_drug_construct2 = NULL, missed_hiv_drug_construct2_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                    puts "........... Updating record into flat_table2 (missed_hiv_drug_construct2): #{patient_id}"

                  end #voided

                when missed_hiv_drug_construct3.blank?
                  if voided.blank?
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET missed_hiv_drug_construct3 = "#{value_text}", missed_hiv_drug_construct3_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                    puts "........... Updating record into flat_table2 (missed_hiv_drug_construct3): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET missed_hiv_drug_construct3 = NULL, missed_hiv_drug_construct3_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                    puts "........... Updating record into flat_table2 (missed_hiv_drug_construct3): #{patient_id}"

                  end #voided

                when missed_hiv_drug_construct4.blank?
                  if voided.blank?
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET missed_hiv_drug_construct4 = "#{value_text}", missed_hiv_drug_construct4_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                    puts "........... Updating record into flat_table2 (missed_hiv_drug_construct4): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET missed_hiv_drug_construct4 = NULL, missed_hiv_drug_construct4_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                    puts "........... Updating record into flat_table2 (missed_hiv_drug_construct4): #{patient_id}"

                  end #voided

                when missed_hiv_drug_construct5.blank?
                  if voided.blank?
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET missed_hiv_drug_construct5 = "#{value_text}", missed_hiv_drug_construct5_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                    puts "........... Updating record into flat_table2 (missed_hiv_drug_construct5): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET missed_hiv_drug_construct5 = NULL, missed_hiv_drug_construct5_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                  end #voided

                end #case

              end #visit

#######################################################################################################################################
              
            when what_was_the_patient_adherence_for_this_drug

              flat_table2_record = Connection.select_one("SELECT * FROM flat_table2 WHERE ID = '#{visit}' ")

              what_was_the_patient_adherence_for_this_drug1 = flat_table2_record['what_was_the_patient_adherence_for_this_drug1']

              what_was_the_patient_adherence_for_this_drug2 = flat_table2_record['what_was_the_patient_adherence_for_this_drug2']

              what_was_the_patient_adherence_for_this_drug3 = flat_table2_record['what_was_the_patient_adherence_for_this_drug3']

              what_was_the_patient_adherence_for_this_drug4 = flat_table2_record['what_was_the_patient_adherence_for_this_drug4']

              what_was_the_patient_adherence_for_this_drug5 = flat_table2_record['what_was_the_patient_adherence_for_this_drug5']

              if visit.blank?
                case 
                when what_was_the_patient_adherence_for_this_drug1.blank?
                  if value_numeric.blank?
                    Connection.execute <<EOF
                    INSERT INTO flat_table2 (patient_id, visit_date, what_was_the_patient_adherence_for_this_drug1, what_was_the_patient_adherence_for_this_drug1_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_text}", "#{encounter_id}");
EOF
                    puts "........... Inserting record into flat_table2 (what_was_the_patient_adherence_for_this_drug1): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    INSERT INTO flat_table2 (patient_id, visit_date, what_was_the_patient_adherence_for_this_drug1, what_was_the_patient_adherence_for_this_drug1_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_numeric}", "#{encounter_id}");
EOF
                    puts "........... Inserting record into flat_table2 (what_was_the_patient_adherence_for_this_drug1): #{patient_id}"

                  end #value_numeric

                when what_was_the_patient_adherence_for_this_drug2.blank?
                  if value_numeric.blank?
                    Connection.execute <<EOF
                    INSERT INTO flat_table2 (patient_id, visit_date, what_was_the_patient_adherence_for_this_drug2, what_was_the_patient_adherence_for_this_drug2_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_text}", "#{encounter_id}");
EOF
                    puts "........... Inserting record into flat_table2 (what_was_the_patient_adherence_for_this_drug2): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    INSERT INTO flat_table2 (patient_id, visit_date, what_was_the_patient_adherence_for_this_drug2, what_was_the_patient_adherence_for_this_drug2_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_numeric}", "#{encounter_id}");
EOF
                    puts "........... Inserting record into flat_table2 (what_was_the_patient_adherence_for_this_drug2): #{patient_id}"

                  end #value_numeric

                when what_was_the_patient_adherence_for_this_drug3.blank?
                  if value_numeric.blank?
                    Connection.execute <<EOF
                    INSERT INTO flat_table2 (patient_id, visit_date, what_was_the_patient_adherence_for_this_drug3, what_was_the_patient_adherence_for_this_drug3_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_text}", "#{encounter_id}");
EOF
                    puts "........... Inserting record into flat_table2 (what_was_the_patient_adherence_for_this_drug3): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    INSERT INTO flat_table2 (patient_id, visit_date, what_was_the_patient_adherence_for_this_drug3, what_was_the_patient_adherence_for_this_drug3_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_numeric}", "#{encounter_id}");
EOF
                    puts "........... Inserting record into flat_table2 (what_was_the_patient_adherence_for_this_drug3): #{patient_id}"

                  end #value_numeric

                when what_was_the_patient_adherence_for_this_drug4.blank?
                  if value_numeric.blank?
                    Connection.execute <<EOF
                    INSERT INTO flat_table2 (patient_id, visit_date, what_was_the_patient_adherence_for_this_drug4, what_was_the_patient_adherence_for_this_drug4_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_text}", "#{encounter_id}");
EOF
                    puts "........... Inserting record into flat_table2 (what_was_the_patient_adherence_for_this_drug4): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    INSERT INTO flat_table2 (patient_id, visit_date, what_was_the_patient_adherence_for_this_drug4, what_was_the_patient_adherence_for_this_drug4_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_numeric}", "#{encounter_id}");
EOF
                    puts "........... Inserting record into flat_table2 (what_was_the_patient_adherence_for_this_drug4): #{patient_id}"

                  end #value_numeric

                when what_was_the_patient_adherence_for_this_drug5.blank?
                  if value_numeric.blank?
                    Connection.execute <<EOF
                    INSERT INTO flat_table2 (patient_id, visit_date, what_was_the_patient_adherence_for_this_drug5, what_was_the_patient_adherence_for_this_drug5_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_text}", "#{encounter_id}");
EOF
                    puts "........... Inserting record into flat_table2 (what_was_the_patient_adherence_for_this_drug5): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    INSERT INTO flat_table2 (patient_id, visit_date, what_was_the_patient_adherence_for_this_drug5, what_was_the_patient_adherence_for_this_drug5_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_numeric}", "#{encounter_id}");
EOF
                    puts "........... Inserting record into flat_table2 (what_was_the_patient_adherence_for_this_drug5): #{patient_id}"

                  end #value_numeric
                end #case

                else

                  case 
                  when what_was_the_patient_adherence_for_this_drug1.blank?
                    if voided.blank?
                      if value_numeric.blank?
                        Connection.execute <<EOF
                        UPDATE flat_table2 SET what_was_the_patient_adherence_for_this_drug1 = "#{value_text}", what_was_the_patient_adherence_for_this_drug1_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                        puts ".......... Updating record into flat_table2 (what_was_the_patient_adherence_for_this_drug1): #{patient_id}"

                      else
                        Connection.execute <<EOF
                        UPDATE flat_table2 SET what_was_the_patient_adherence_for_this_drug1 = "#{value_numeric}", what_was_the_patient_adherence_for_this_drug1_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                        puts ".......... Updating record into flat_table2 (what_was_the_patient_adherence_for_this_drug1): #{patient_id}"

                      end #value_numeric

                    else
                      Connection.execute <<EOF
                      UPDATE flat_table2 SET what_was_the_patient_adherence_for_this_drug1 = NULL, what_was_the_patient_adherence_for_this_drug1_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                      puts ".......... Updating record into flat_table2 (what_was_the_patient_adherence_for_this_drug1): #{patient_id}"

                    end #voided

                  when what_was_the_patient_adherence_for_this_drug2
                    if voided.blank?
                      if value_numeric.blank?
                        Connection.execute <<EOF
                        UPDATE flat_table2 SET what_was_the_patient_adherence_for_this_drug2 = "#{value_text}", what_was_the_patient_adherence_for_this_drug2_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                        puts ".......... Updating record into flat_table2 (what_was_the_patient_adherence_for_this_drug2): #{patient_id}"

                      else
                        Connection.execute <<EOF
                        UPDATE flat_table2 SET what_was_the_patient_adherence_for_this_drug2 = "#{value_numeric}", what_was_the_patient_adherence_for_this_drug2_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                        puts ".......... Updating record into flat_table2 (what_was_the_patient_adherence_for_this_drug2): #{patient_id}"

                      end #value_numeric

                    else
                      Connection.execute <<EOF
                      UPDATE flat_table2 SET what_was_the_patient_adherence_for_this_drug2 = NULL, what_was_the_patient_adherence_for_this_drug2_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                      puts ".......... Updating record into flat_table2 (what_was_the_patient_adherence_for_this_drug2): #{patient_id}"

                    end #voided

                  when what_was_the_patient_adherence_for_this_drug3.blank?
                    if voided.blank?
                      if value_numeric.blank?
                        Connection.execute <<EOF
                        UPDATE flat_table2 SET what_was_the_patient_adherence_for_this_drug3 = "#{value_text}", what_was_the_patient_adherence_for_this_drug3_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                        puts ".......... Updating record into flat_table2 (what_was_the_patient_adherence_for_this_drug3): #{patient_id}"

                      else
                        Connection.execute <<EOF
                        UPDATE flat_table2 SET what_was_the_patient_adherence_for_this_drug3 = "#{value_numeric}", what_was_the_patient_adherence_for_this_drug3_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                        puts ".......... Updating record into flat_table2 (what_was_the_patient_adherence_for_this_drug3): #{patient_id}"

                      end #value_numeric

                    else
                      Connection.execute <<EOF
                      UPDATE flat_table2 SET what_was_the_patient_adherence_for_this_drug3 = NULL, what_was_the_patient_adherence_for_this_drug3_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                      puts ".......... Updating record into flat_table2 (what_was_the_patient_adherence_for_this_drug3): #{patient_id}"

                    end #voided

                  when what_was_the_patient_adherence_for_this_drug4.blank?
                    if voided.blank?
                      if value_numeric.blank?
                        Connection.execute <<EOF
                        UPDATE flat_table2 SET what_was_the_patient_adherence_for_this_drug4 = "#{value_text}", what_was_the_patient_adherence_for_this_drug4_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                        puts ".......... Updating record into flat_table2 (what_was_the_patient_adherence_for_this_drug4): #{patient_id}"

                      else
                        Connection.execute <<EOF
                        UPDATE flat_table2 SET what_was_the_patient_adherence_for_this_drug4 = "#{value_numeric}", what_was_the_patient_adherence_for_this_drug4_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                        puts ".......... Updating record into flat_table2 (what_was_the_patient_adherence_for_this_drug4): #{patient_id}"

                      end #value_numeric

                    else
                      Connection.execute <<EOF
                      UPDATE flat_table2 SET what_was_the_patient_adherence_for_this_drug4 = NULL, what_was_the_patient_adherence_for_this_drug4_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                      puts ".......... Updating record into flat_table2 (what_was_the_patient_adherence_for_this_drug4): #{patient_id}"

                    end #voided

                  when what_was_the_patient_adherence_for_this_drug5.blank?
                    if voided.blank?
                      if value_numeric.blank?
                        Connection.execute <<EOF
                        UPDATE flat_table2 SET what_was_the_patient_adherence_for_this_drug5 = "#{value_text}", what_was_the_patient_adherence_for_this_drug5_enc_id = "#{encounter_id}" WHERE flat_table2.id = "visit";
EOF
                        puts ".......... Updating record into flat_table2 (what_was_the_patient_adherence_for_this_drug5): #{patient_id}"

                      else
                        Connection.execute <<EOF
                        UPDATE flat_table2 SET what_was_the_patient_adherence_for_this_drug5 = "#{value_numeric}", what_was_the_patient_adherence_for_this_drug5_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                        puts ".......... Updating record into flat_table2 (what_was_the_patient_adherence_for_this_drug5): #{patient_id}"

                      end #value_numeric

                    else
                      Connection.execute <<EOF
                      UPDATE flat_table2 SET what_was_the_patient_adherence_for_this_drug5 = NULL, what_was_the_patient_adherence_for_this_drug5_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                      puts ".......... Updating record into flat_table2 (what_was_the_patient_adherence_for_this_drug5): #{patient_id}"

                    end #voided

                  end #case

              end #visit   

 ##############################################################################################################################         

            when amount_of_drug_remaining_at_home

              flat_table2_record = Connection.select_one("SELECT * FROM flat_table2 WHERE ID = '#{Current_date}' ")

              amount_of_drug1_remaining_at_home = flat_table2_record['amount_of_drug1_remaining_at_home']

              amount_of_drug2_remaining_at_home = flat_table2_record['amount_of_drug2_remaining_at_home']

              amount_of_drug3_remaining_at_home = flat_table2_record['amount_of_drug3_remaining_at_home']

              amount_of_drug4_remaining_at_home = flat_table2_record['amount_of_drug4_remaining_at_home']

              amount_of_drug5_remaining_at_home = flat_table2_record['amount_of_drug5_remaining_at_home']

              if visit.blank?
                case 
                when amount_of_drug1_remaining_at_home.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, amount_of_drug1_remaining_at_home, amount_of_drug1_remaining_at_home_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_numeric}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (amount_of_drug1_remaining_at_home): #{patient_id}"

                when amount_of_drug2_remaining_at_home.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, amount_of_drug2_remaining_at_home, amount_of_drug2_remaining_at_home_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_numeric}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (amount_of_drug2_remaining_at_home): #{patient_id}"

                when amount_of_drug3_remaining_at_home.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, amount_of_drug3_remaining_at_home, amount_of_drug3_remaining_at_home_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_numeric}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (amount_of_drug3_remaining_at_home): #{patient_id}"

                when amount_of_drug4_remaining_at_home.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, amount_of_drug4_remaining_at_home, amount_of_drug4_remaining_at_home_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_numeric}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (amount_of_drug4_remaining_at_home): #{patient_id}"

                when amount_of_drug5_remaining_at_home.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, amount_of_drug5_remaining_at_home, amount_of_drug5_remaining_at_home_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_numeric}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (amount_of_drug5_remaining_at_home): #{patient_id}"

                end #case

              else

                case 
                when amount_of_drug1_remaining_at_home.blank?
                  if voided.blank?
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug1_remaining_at_home = "#{value_numeric}", amount_of_drug1_remaining_at_home_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug1_remaining_at_home): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug1_remaining_at_home = NULL, amount_of_drug1_remaining_at_home_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug1_remaining_at_home): #{patient_id}"

                  end #voided

                when amount_of_drug2_remaining_at_home.blank?
                  if voided.blank
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug2_remaining_at_home = "#{value_numeric}", amount_of_drug2_remaining_at_home_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug2_remaining_at_home): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug2_remaining_at_home = NULL, amount_of_drug2_remaining_at_home_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug2_remaining_at_home): #{patient_id}"

                  end #voided

                when amount_of_drug3_remaining_at_home.blank?
                  if voided.blank?
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug3_remaining_at_home = "#{value_numeric}", amount_of_drug3_remaining_at_home_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug3_remaining_at_home): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug3_remaining_at_home = NULL, amount_of_drug3_remaining_at_home_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug3_remaining_at_home): #{patient_id}"

                  end #voided

                when amount_of_drug4_remaining_at_home.blank?
                  if voided.blank?
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug4_remaining_at_home = "#{value_numeric}", amount_of_drug4_remaining_at_home_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug4_remaining_at_home): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug4_remaining_at_home = NULL, amount_of_drug4_remaining_at_home_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug4_remaining_at_home): #{patient_id}"

                  end #voided

                when amount_of_drug5_remaining_at_home.blank?
                  if voided.blank?
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug5_remaining_at_home = "#{value_numeric}", amount_of_drug5_remaining_at_home_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug5_remaining_at_home): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug5_remaining_at_home = NULL, amount_of_drug5_remaining_at_home_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug5_remaining_at_home): #{patient_id}"

                  end #voided
                end #case
              end #visit

 ##################################################################################################################################

            when amount_of_drug_brought_to_clinic

              flat_table2_record = Connection.select_one("SELECT * FROM flat_table2 WHERE ID = '#{visit}' ")

              amount_of_drug1_brought_to_clinic = flat_table2_record['amount_of_drug1_brought_to_clinic']

              amount_of_drug2_brought_to_clinic = flat_table2_record['amount_of_drug2_brought_to_clinic']

              amount_of_drug3_brought_to_clinic = flat_table2_record['amount_of_drug3_brought_to_clinic']

              amount_of_drug4_brought_to_clinic = flat_table2_record['amount_of_drug4_brought_to_clinic']

              amount_of_drug5_brought_to_clinic = flat_table2_record['amount_of_drug5_brought_to_clinic']

              if visit.blank?
                case 
                when amount_of_drug1_brought_to_clinic.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, amount_of_drug1_brought_to_clinic, amount_of_drug1_brought_to_clinic_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_numeric}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (amount_of_drug1_brought_to_clinic): #{patient_id}"

                when amount_of_drug2_brought_to_clinic.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, amount_of_drug2_brought_to_clinic, amount_of_drug2_brought_to_clinic_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_numeric}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (amount_of_drug2_brought_to_clinic): #{patient_id}"

                when amount_of_drug3_brought_to_clinic.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, amount_of_drug3_brought_to_clinic, amount_of_drug3_brought_to_clinic_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_numeric}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (amount_of_drug3_brought_to_clinic): #{patient_id}"

                when amount_of_drug4_brought_to_clinic.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, amount_of_drug4_brought_to_clinic, amount_of_drug4_brought_to_clinic_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_numeric}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (amount_of_drug4_brought_to_clinic): #{patient_id}"

                when amount_of_drug5_brought_to_clinic.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, amount_of_drug5_brought_to_clinic, amount_of_drug5_brought_to_clinic_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_numeric}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (amount_of_drug5_brought_to_clinic): #{patient_id}"

                end #case

              else

                case 
                when amount_of_drug1_brought_to_clinic.blank?
                  if voided.blank?
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug1_brought_to_clinic = "#{value_numeric}", amount_of_drug1_brought_to_clinic_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug1_brought_to_clinic): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug1_brought_to_clinic = NULL, amount_of_drug1_brought_to_clinic_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug1_brought_to_clinic): #{patient_id}"

                  end #voided

                when amount_of_drug2_brought_to_clinic.blank?
                  if voided.blank?
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug2_brought_to_clinic = "#{value_numeric}", amount_of_drug2_brought_to_clinic_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug2_brought_to_clinic): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug2_brought_to_clinic = NULL, amount_of_drug2_brought_to_clinic_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug2_brought_to_clinic): #{patient_id}"

                  end #voided

                when amount_of_drug3_brought_to_clinic.blank?
                  if voided.blank?
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug3_brought_to_clinic = "#{value_numeric}", amount_of_drug3_brought_to_clinic_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug3_brought_to_clinic): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug3_brought_to_clinic = NULL, amount_of_drug3_brought_to_clinic_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug3_brought_to_clinic): #{patient_id}"

                  end #voided

                when amount_of_drug4_brought_to_clinic.blank?
                  if voided.blank?
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug4_brought_to_clinic = "#{value_numeric}", amount_of_drug4_brought_to_clinic_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug4_brought_to_clinic): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug4_brought_to_clinic = NULL, amount_of_drug4_brought_to_clinic_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug4_brought_to_clinic): #{patient_id}"

                  end #voided

                when amount_of_drug5_brought_to_clinic.blank?
                  if voided.blank?
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug5_brought_to_clinic = "#{value_numeric}", amount_of_drug5_brought_to_clinic_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug5_brought_to_clinic): #{patient_id}"

                  else
                    Connection.execute <<EOF
                    UPDATE flat_table2 SET amount_of_drug5_brought_to_clinic = NULL, amount_of_drug5_brought_to_clinic_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                    puts ".......... Updating record into flat_table2 (amount_of_drug5_brought_to_clinic): #{patient_id}"

                  end #voided
                end #case
              end #visit

 ######################################################################################################################################

            when malawi_ART_side_effects
              value_record = Connection.select_one("SELECT name FROM concept_name WHERE concept_name_id = '#{value_coded_name_id}' ")
              value = value_record['name']

              if voided.blank?
                if visit.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, malawi_ART_side_effects, malawi_ART_side_effects_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (malawi_ART_side_effects): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET malawi_ART_side_effects = "#{value}", malawi_ART_side_effects_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                  puts ".......... Updating record into flat_table2 (malawi_ART_side_effects): #{patient_id}"

                end #visit

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET malawi_ART_side_effects = NULL, malawi_ART_side_effects_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts ".......... Updating record into flat_table2 (malawi_ART_side_effects): #{patient_id}"

              end #voided

 ######################################################################################################################################

            when condoms
              if voided.blank?
                if visit.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, condoms_given, condoms_given_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_text}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (Condoms): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET condoms_given = "#{value_numeric}", condoms_given_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                  puts ".......... Updating record into flat_table2 (Condoms): #{patient_id}"

                end #visit

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET condoms_given = NULL, condoms_given_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts ".......... Updating record into flat_table2 (Condoms): #{patient_id}"

              end #voided

 ####################################################################################################################################

            when appointment_date
              if voided.blank?
                if visit.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, appointment_date, appointment_date_enc_id) VALUES ("#{patient_id}", '#{Current_date}', '#{value_datetime}', "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (appointment_date): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET appointment_date = '#{value_datetime}', appointment_date_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                  puts ".......... Updating record into flat_table2 (appointment_date): #{patient_id}"

                end #visit

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET appointment_date = NULL, appointment_date_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts ".......... Updating record into flat_table2 (appointment_date): #{patient_id}"

              end #voided

 #####################################################################################################################################

            when transfer_out_location
              if voided.blank?
                if visit.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, transfer_out_location, transfer_out_location_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{value_text}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (transfer_out_location): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET transfer_out_location = "#{value_text}", transfer_out_location_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                  puts ".......... Updating record into flat_table2 (transfer_out_location): #{patient_id}"

                end #visit

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET transfer_out_location = NULL, transfer_out_location_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts ".......... Updating record into flat_table2 (transfer_out_location): #{patient_id}"

              end #voided

 ####################################################################################################################################

            when regimen_category

              reg_category_record = Connection.select_one("SELECT obs.value_text FROM obs obs INNER JOIN encounter e ON e.encounter_id = obs.encounter_id and e.encounter_type = 54 WHERE obs.person_id = '#{patient_id}' AND obs.encounter_id = '#{encounter_id}' AND DATE(obs.obs_datetime) = DATE(#{'Current_date'} ")

              reg_category = reg_category_record['value_text'] rescue nil

              if reg_category.blank?
                reg_category = value_text
              end #reg_category

              if voided.blank?
                if visit.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, regimen_category, regimen_category_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{reg_category}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (regimen_category): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET regimen_category = "#{reg_category}", regimen_category_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                  puts ".......... Updating record into flat_table2 (regimen_category): #{patient_id}"

                end #visit

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET regimen_category = NULL, regimen_category_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts ".......... Updating record into flat_table2 (regimen_category): #{patient_id}"

              end #voided

 ####################################################################################################################################

            when weight
              if voided.blank?
                if value_numeric.blank?
                  wt_value = value_text
                else
                  wt_value = value_numeric
                end #value_numeric

                if visit.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, weight, weight_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{wt_value}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (weight): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET weight = "#{wt_value}", weight_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                  puts ".......... Updating record into flat_table2 (weight): #{patient_id}"

                end #visit

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET weight = NULL, weight_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts ".......... Updating record into flat_table2 (weight): #{patient_id}"

              end #voided

 #####################################################################################################################################

            when temperature
              if voided.blank?
                if value_numeric.blank?
                  temp_value = value_text

                else
                  temp_value = value_numeric
                end #value_numeric

                if visit.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, temperature, temperature_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{temp_value}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (temperature): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET temperature = "#{temp_value}", temperature_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                  puts ".......... Updating record into flat_table2 (temperature): #{patient_id}"

                end #visit

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET temperature = NULL, temperature_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts ".......... Updating record into flat_table2 (temperature): #{patient_id}"

              end #voided
              
  ####################################################################################################################################

            when bmi
              if voided.blank?
                if value_numeric.blank?
                  bmi_value = value_text

                else
                  bmi_value = value_numeric
                end #value_numeric

                if visit.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, bm, bmi_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{bmi_value}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (BMI): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET bmi = "#{bmi_value}", bmi_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                  puts ".......... Updating record into flat_table2 (BMI): #{patient_id}"

                end #visit

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET bmi = NULL, bmi_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts ".......... Updating record into flat_table2 (BMI): #{patient_id}"

              end #voided

            when systolic_blood_pressure

              if voided.blank?
                
                if value_numeric.blank?
                  sys_value = value_text
                else
                  sys_value = value_numeric
                end #value_numeric

                if visit.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, systolic_blood_pressure, systolic_blood_pressure_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{sys_value}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (systolic_blood_pressure): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET systolic_blood_pressure = "#{sys_value}", systolic_blood_pressure_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                  puts ".......... Updating record into flat_table2 (systolic_blood_pressure): #{patient_id}"

                end #visit

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET systolic_blood_pressure = NULL, systolic_blood_pressure_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts ".......... Updating record into flat_table2 (systolic_blood_pressure): #{patient_id}"

              end #voided

 ####################################################################################################################################

            when diastolic_blood_pressure
              if voided.blank?
                if value_numeric.blank?
                  dia_value = value_text

                else
                  dia_value = value_numeric
                end #value_numeric

                if visit.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, diastolic_blood_pressure, diastolic_blood_pressure_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{dia_value}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (diastolic_blood_pressure): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET diastolic_blood_pressure = "#{dia_value}", diastolic_blood_pressure_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                  puts ".......... Updating record into flat_table2 (diastolic_blood_pressure): #{patient_id}"

                end #visit

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET diastolic_blood_pressure = NULL, diastolic_blood_pressure_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts ".......... Updating record into flat_table2 (diastolic_blood_pressure): #{patient_id}"

              end #voided

  #####################################################################################################################################

            when weight_for_height
              if voided.blank?
                if value_numeric.blank?
                  wt4ht_value = value_text

                else
                  wt4ht_value = value_numeric
                end #value_numeric

                if visit.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, weight_for_height, weight_for_height_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{wt4ht_value}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (weight_for_height): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET weight_for_height = "#{wt4ht_value}", weight_for_height_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                  puts ".......... Updating record into flat_table2 (weight_for_height): #{patient_id}"

                end #visit

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET weight_for_height = NULL, weight_for_height_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts ".......... Updating record into flat_table2 (weight_for_height): #{patient_id}"

              end #voided

  ####################################################################################################################################
            
            when weight_for_age
              if voided.blank?
                if value_numeric.blank?
                  wt4age = value_text

                else
                  wt4age = value_numeric
                end #value_numeric

                if visit.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, weight_for_age, weight_for_age_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{wt4age}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (weight_for_age): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET weight_for_age = "#{wt4age}", weight_for_age_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                  puts ".......... Updating record into flat_table2 (weight_for_age): #{patient_id}"

                end #visit

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET weight_for_age = NULL, weight_for_age_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts ".......... Updating record into flat_table2 (weight_for_age): #{patient_id}"

              end #voided

  ####################################################################################################################################

            when height_for_age
              if voided.blank?
                if value_numeric.blank?
                  ht4age_value = value_text

                else
                  ht4age_value = value_numeric
                end #value_numeric

                if visit.blank?
                  Connection.execute <<EOF
                  INSERT INTO flat_table2 (patient_id, visit_date, height_for_age, height_for_age_enc_id) VALUES ("#{patient_id}", '#{Current_date}', "#{ht4age_value}", "#{encounter_id}");
EOF
                  puts ".......... Inserting record into flat_table2 (height_for_age): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table2 SET height_for_age = "#{ht4age_value}", height_for_age_enc_id = "#{encounter_id}" WHERE flat_table2.id = "#{visit}";
EOF
                  puts ".......... Updating record into flat_table2 (height_for_age): #{patient_id}"

                end #visit

              else
                Connection.execute <<EOF
                UPDATE flat_table2 SET height_for_age = NULL, height_for_age_enc_id = NULL WHERE flat_table2.id = "#{visit}";
EOF
                puts ".......... Updating record into flat_table2 (height_for_age): #{patient_id}"

              end #voided

  ####################################################################################################################################

            when ever_received_art
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id WHERE concept.concept_id = in_field_value_coded AND voided = 0 AND retired = 0") 
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1 SET ever_received_art = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (ever_received_art): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1 SET ever_received_art = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (ever_received_art): #{patient_id}"

              end #voided
              
           

  ###################################################################################################################################

            when date_last_taken_arv
              if voided.blank?
                Connection.execute <<EOF
                UPDATE flat_table1 SET date_art_last_taken = '#{value_datetime}', date_art_last_taken_v_date = '#{Current_date}' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (date_last_taken_arv): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET date_art_last_taken = NULL, date_art_last_taken_v_date = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (date_last_taken_arv): #{patient_id}"

              end #voided

  ##################################################################################################################################

            when art_in_2_months
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET taken_art_in_last_two_months = "#{answer}", taken_art_in_last_two_months_v_date = '#{Current_date}' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (art_in_2_months): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET taken_art_in_last_two_months = NULL, taken_art_in_last_two_months_v_date = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (art_in_2_months): #{patient_id}"

              end #voided

  ###################################################################################################################################

            when art_in_2_weeks
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET taken_art_in_last_two_weeks = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (art_in_2_weeks): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET taken_art_in_last_two_weeks = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (art_in_2_weeks): #{patient_id}"

              end #voided

  ##################################################################################################################################

            when last_arv_reg
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET last_art_drugs_taken = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (last_arv_reg): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET last_art_drugs_taken = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (last_arv_reg): #{patient_id}"

              end #voided

  ##################################################################################################################################

            when ever_reg_4_art
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET ever_registered_at_art_clinic = "#{answer}", ever_registered_at_art_v_date = '#{Current_date}' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (ever_reg_4_art): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET ever_registered_at_art_clinic = NULL, ever_registered_at_art_v_date = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (ever_reg_4_art): #{patient_id}"

              end #voided

  ###################################################################################################################################
            
            when has_transfer_letter
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET has_transfer_letter = "#{answer}" WHERE flat_table1.patient_id = "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (has_transfer_letter): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET has_transfer_letter = NULL WHERE flat_table1.patient_id = "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (has_transfer_letter): #{patient_id}"

              end #voided

  ###################################################################################################################################
            

            when art_init_loc
              if voided.blank?
                answer_record = Connection.select_one("SELECT name FROM location WHERE location_id = '#{value_text}' ")

                answer = answer_record['name'] rescue nil

                if answer.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET location_of_art_initialization = "Unknown" WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
                  puts ".......... Updating record into flat_table1 (art_init_loc): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET location_of_art_initialization = "#{answer}" WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
                  puts ".......... Updating record into flat_table1 (art_init_loc): #{patient_id}"

                end #answer

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET location_of_art_initialization = NULL WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
                puts ".......... Updating record into flat_table1 (art_init_loc): #{patient_id}"

              end #voided

  ##################################################################################################################################

            when date_started_art
              if voided.blank?
                Connection.execute <<EOF
                UPDATE flat_table1 SET date_started_art = '#{value_datetime}' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (date_started_art): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET date_started_art = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (date_started_art): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when cd4_count_loc
              if voided.blank?
                answer_record = Connection.select_one("SELECT name FROM location WHERE location_id = '#{value_text}' ")

                answer = answer_record['name'] rescue nil

                if answer.blank?
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET cd4_count_location = "Unknown" WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
                puts ".......... Updating record into flat_table1 (cd4_count_loc): #{patient_id}"

                else
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET cd4_count_location = "#{answer}" WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
                  puts ".......... Updating record into flat_table1 (cd4_count_loc): #{patient_id}"

                end #answer

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET cd4_count_location = NULL WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
                puts ".......... Updating record into flat_table1 (cd4_count_loc): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when cd4_percent_loc
              if voided.blank?
                Connection.execute <<EOF
                UPDATE flat_table1 SET cd4_count_location = "#{value_text}" WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
                puts ".......... Updating record into flat_table1 (cd4_percent_loc): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET cd4_count_location = NULL WHERE flat_table1.patient_id = "#{patient_id}" ;
EOF
                puts ".......... Updating record into flat_table1 (cd4_percent_loc): #{patient_id}"

              end #voided

  ##################################################################################################################################

            when cd4_count_date
              if voided.blank?
                Connection.execute <<EOF
                UPDATE flat_table1 SET cd4_count_datetime = '#{value_datetime}' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cd4_count_date): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET cd4_count_datetime = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cd4_count_date): #{patient_id}"

              end #voided

  ##################################################################################################################################

            when cd4_count_percent
              Connection.execute <<EOF
              UPDATE flat_table1 SET cd4_count_percent = "#{value_numeric}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
              puts ".......... Updating record into flat_table1 (cd4_count_percent): #{patient_id}"

  #################################################################################################################################

            when cd4_percent_less_than_25
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")

                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET cd4_percent_less_than_25 = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cd4_percent_less_than_25): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET cd4_percent_less_than_25 = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cd4_percent_less_than_25): #{patient_id}"

              end #voided

  #################################################################################################################################

            when cd4_count_less_than_250
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']
              
                Connection.execute <<EOF
                UPDATE flat_table1 SET cd4_count_less_than_250 = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cd4_count_less_than_250): #{patient_id}"
              
              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET cd4_count_less_than_250 = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cd4_count_less_than_250): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when cd4_count_less_than_350
              if voided.blank?
                 answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                 answer = answer_record['name']

                 Connection.execute <<EOF
                 UPDATE flat_table1 SET cd4_count_less_than_350 = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cd4_count_less_than_350): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET cd4_count_less_than_350 = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cd4_count_less_than_350): #{patient_id}"

              end #voided

  ##################################################################################################################################

            when lymphocyte_count_date
              if voided.blank?
                Connection.execute <<EOF
                UPDATE flat_table1 SET lymphocyte_count_date = "#{value_datetime}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (lymphocyte_count_date): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET lymphocyte_count_date = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (lymphocyte_count_date): #{patient_id}"

              end #voided

  ##################################################################################################################################

            when lymphocyte_count
              if voided.blank?
                Connection.execute <<EOF
                UPDATE flat_table1 SET lymphocyte_count = "#{value_numeric}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (lymphocyte_count): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET lymphocyte_count = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (lymphocyte_count): #{patient_id}"

              end #voided

  ##################################################################################################################################

            when asymptomatic
              if voided.blank?
                answer = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET asymptomatic = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (asymptomatic): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET asymptomatic = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (asymptomatic): #{patient_id}"

              end #voided

  ##################################################################################################################################

            when pers_gnrl_lymphadenopathy
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET persistent_generalized_lymphadenopathy= "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (pers_gnrl_lymphadenopathy): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET persistent_generalized_lymphadenopathy = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (pers_gnrl_lymphadenopathy): #{patient_id}"

              end #voided

  ###################################################################################################################################
            
            when unspecified_stage_1_cond
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET unspecified_stage_1_cond = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (unspecified_stage_1_cond): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET unspecified_stage_1_cond = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (unspecified_stage_1_cond): #{patient_id}"

              end #voided

  ##################################################################################################################################

            when molluscumm_contagiosum
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET molluscumm_contagiosum = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (molluscumm_contagiosum): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET molluscumm_contagiosum = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (molluscumm_contagiosum): #{patient_id}"

              end #voided

  #################################################################################################################################

            when wart_virus_infection_extensive
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET wart_virus_infection_extensive = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (wart_virus_infection_extensive): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET wart_virus_infection_extensive = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (wart_virus_infection_extensive): #{patient_id}"

              end #voided

  #################################################################################################################################

            when oral_ulcerations_recurrent
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET oral_ulcerations_recurrent = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (oral_ulcerations_recurrent): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET oral_ulcerations_recurrent = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (oral_ulcerations_recurrent): #{patient_id}"

              end #voided

  #################################################################################################################################
            
            when parotid_enlargement_pers_unexp
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET parotid_enlargement_persistent_unexplained = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (parotid_enlargement_pers_unexp): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET parotid_enlargement_persistent_unexplained = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (parotid_enlargement_pers_unexp): #{patient_id}"

              end #voided
              
  #################################################################################################################################
            
            when lineal_gingival_erythema
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET lineal_gingival_erythema = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (lineal_gingival_erythema): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET lineal_gingival_erythema = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (lineal_gingival_erythema): #{patient_id}"

              end #voided

  ###############################################################################################################################
            
            when herpes_zoster
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET herpes_zoster = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (herpes_zoster): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET herpes_zoster = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (herpes_zoster): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when resp_tract_infections_rec
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET respiratory_tract_infections_recurrent = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (resp_tract_infections_rec): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET respiratory_tract_infections_recurrent = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (resp_tract_infections_rec): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when unspecified_stage2_condition
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET unspecified_stage2_condition = "#{answer}" WHERE flat_table1.patient_id= in_patient_id;
EOF
                puts ".......... Updating record into flat_table1 (unspecified_stage2_condition): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET unspecified_stage2_condition = NULL WHERE flat_table1.patient_id= in_patient_id;
EOF
                puts ".......... Updating record into flat_table1 (unspecified_stage2_condition): #{patient_id}"

              end #voided

  ###################################################################################################################################

            when angular_chelitis
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET angular_chelitis = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (angular_chelitis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET angular_chelitis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (angular_chelitis): #{patient_id}"

              end #voided

  ##################################################################################################################################

            when papular_prurtic_eruptions
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET papular_pruritic_eruptions = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (papular_prurtic_eruptions): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET papular_pruritic_eruptions = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (papular_prurtic_eruptions): #{patient_id}"

              end #voided

  ##################################################################################################################################

            when hepatosplenomegaly_unexplained
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET hepatosplenomegaly_unexplained = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (hepatosplenomegaly_unexplained): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET hepatosplenomegaly_unexplained = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (hepatosplenomegaly_unexplained): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when oral_hairy_leukoplakia
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET oral_hairy_leukoplakia = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (oral_hairy_leukoplakia): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET oral_hairy_leukoplakia = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (oral_hairy_leukoplakia): #{patient_id}"

              end #voided

  #################################################################################################################################

            when severe_weight_loss
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET severe_weight_loss = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (severe_weight_loss): #{patient_id}"

                Connection.execute <<EOF
                UPDATE flat_table1 SET severe_weight_loss = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (severe_weight_loss): #{patient_id}"

              end #voided
              
  ##################################################################################################################################
            
            when fever_persistent_unexplained
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET fever_persistent_unexplained = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (fever_persistent_unexplained): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET fever_persistent_unexplained = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (fever_persistent_unexplained): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when pulmonary_tuberculosis
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET pulmonary_tuberculosis = "#{answer}", pulmonary_tuberculosis_v_date = '#{Current_date}' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (pulmonary_tuberculosis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET pulmonary_tuberculosis = NULL, pulmonary_tuberculosis_v_date = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (pulmonary_tuberculosis): #{patient_id}"

              end #voided

  #################################################################################################################################

            when pulmonary_tuberculosis_last_2_years
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET pulmonary_tuberculosis_last_2_years = "#{answer}", pulmonary_tuberculosis_last_2_years_v_date = '#{Current_date}' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (pulmonary_tuberculosis_last_2_years): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET pulmonary_tuberculosis_last_2_years = NULL, pulmonary_tuberculosis_last_2_years_v_date = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (pulmonary_tuberculosis_last_2_years): #{patient_id}"

              end #voided

  ##################################################################################################################################

            when severe_bacterial_infection
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET severe_bacterial_infection = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (severe_bacterial_infection): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET severe_bacterial_infection = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (severe_bacterial_infection): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when bacterial_pnuemonia
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET bacterial_pnuemonia = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (bacterial_pnuemonia): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET bacterial_pnuemonia = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (bacterial_pnuemonia): #{patient_id}"

              end #voided
              
  ###############################################################################################################################

            when symptomatic_lymphoid_interstitial_pnuemonitis
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET symptomatic_lymphoid_interstitial_pnuemonitis = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (symptomatic_lymphoid_interstitial_pnuemonitis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET symptomatic_lymphoid_interstitial_pnuemonitis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (symptomatic_lymphoid_interstitial_pnuemonitis): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when chronic_hiv_assoc_lung_disease
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET chronic_hiv_assoc_lung_disease = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (chronic_hiv_assoc_lung_disease): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET chronic_hiv_assoc_lung_disease = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (chronic_hiv_assoc_lung_disease): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when unspecified_stage3_condition
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET unspecified_stage3_condition = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (unspecified_stage3_condition): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET unspecified_stage3_condition = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (unspecified_stage3_condition): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when aneamia
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET aneamia = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (aneamia): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET aneamia = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (aneamia): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when neutropaenia
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET neutropaenia = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (neutropaenia): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET neutropaenia = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (neutropaenia): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when thrombocytopaenia_chronic
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET thrombocytopaenia_chronic = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (thrombocytopaenia_chronic): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET thrombocytopaenia_chronic = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (thrombocytopaenia_chronic): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when diarhoea
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET diarhoea = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (diarhoea): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET diarhoea = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (diarhoea): #{patient_id}"

              end #voided
              
  ###################################################################################################################################
            
            when oral_candidiasis
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET oral_candidiasis = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (oral_candidiasis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET oral_candidiasis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (oral_candidiasis): #{patient_id}"

              end #voided

  ##################################################################################################################################
            
            when acute_necrotizing_ulcerative_gingivitis
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET acute_necrotizing_ulcerative_gingivitis = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (acute_necrotizing_ulcerative_gingivitis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET acute_necrotizing_ulcerative_gingivitis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (acute_necrotizing_ulcerative_gingivitis): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when lymph_node_tuberculosis
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET lymph_node_tuberculosis = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (lymph_node_tuberculosis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET lymph_node_tuberculosis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (lymph_node_tuberculosis): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when toxoplasmosis_of_brain
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET toxoplasmosis_of_the_brain  = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (toxoplasmosis_of_brain): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET toxoplasmosis_of_the_brain  = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (toxoplasmosis_of_brain): #{patient_id}"

              end #voided
              
  ##################################################################################################################################
            
            when cryptococcal_meningitis
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET cryptococcal_meningitis = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cryptococcal_meningitis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET cryptococcal_meningitis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cryptococcal_meningitis): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when progressive_multifocal_leukoencephalopathy
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET progressive_multifocal_leukoencephalopathy = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (progressive_multifocal_leukoencephalopathy): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET progressive_multifocal_leukoencephalopathy = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (progressive_multifocal_leukoencephalopathy): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when disseminated_mycosis
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET disseminated_mycosis = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (disseminated_mycosis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET disseminated_mycosis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (disseminated_mycosis): #{patient_id}"

              end #voided
              
  ###################################################################################################################################

            when candidiasis_of_oesophagus
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET candidiasis_of_oesophagus = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (disseminated_mycosis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET candidiasis_of_oesophagus = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (disseminated_mycosis): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when extrapulmonary_tuberculosis
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET extrapulmonary_tuberculosis = "#{answer}", extrapulmonary_tuberculosis_v_date = '#{Current_date}' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (extrapulmonary_tuberculosis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET extrapulmonary_tuberculosis = NULL, extrapulmonary_tuberculosis_v_date = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (extrapulmonary_tuberculosis): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when cerebral_non_hodgkin_lymphoma
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET cerebral_non_hodgkin_lymphoma = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cerebral_non_hodgkin_lymphoma): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET cerebral_non_hodgkin_lymphoma = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cerebral_non_hodgkin_lymphoma): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when hiv_encephalopathy
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET hiv_encephalopathy = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (hiv_encephalopathy): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET hiv_encephalopathy = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (hiv_encephalopathy): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when bacterial_infections_severe_recurrent
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET bacterial_infections_severe_recurrent = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (bacterial_infections_severe_recurrent): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET bacterial_infections_severe_recurrent = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (bacterial_infections_severe_recurrent): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when unspecified_stage_4_condition
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET unspecified_stage_4_condition = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (unspecified_stage_4_condition): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET unspecified_stage_4_condition = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (unspecified_stage_4_condition): #{patient_id}"

              end #voided
            
  #################################################################################################################################
            
            when pnuemocystis_pnuemonia
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET pnuemocystis_pnuemonia = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (pnuemocystis_pnuemonia): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET pnuemocystis_pnuemonia = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (pnuemocystis_pnuemonia): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when disseminated_non_tuberculosis_mycobactierial_infection
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET disseminated_non_tuberculosis_mycobacterial_infection = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (disseminated_non_tuberculosis_mycobactierial_infection): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET disseminated_non_tuberculosis_mycobacterial_infection = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (disseminated_non_tuberculosis_mycobactierial_infection): #{patient_id}"

              end #voided
              
  #################################################################################################################################
            
            when cryptosporidiosis
              if voided.blank
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET cryptosporidiosis = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cryptosporidiosis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET cryptosporidiosis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cryptosporidiosis): #{patient_id}"

              end #voided
  
  ################################################################################################################################

            when isosporiasis
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET isosporiasis = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (isosporiasis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET isosporiasis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (isosporiasis): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when symptomatic_hiv_asscoiated_nephropathy
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET symptomatic_hiv_associated_nephropathy = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (symptomatic_hiv_asscoiated_nephropathy): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET symptomatic_hiv_associated_nephropathy = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (symptomatic_hiv_asscoiated_nephropathy): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when chronic_herpes_simplex_infection
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET chronic_herpes_simplex_infection = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (chronic_herpes_simplex_infection): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET chronic_herpes_simplex_infection = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (chronic_herpes_simplex_infection): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when cytomegalovirus_infection
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET cytomegalovirus_infection = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cytomegalovirus_infection): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET cytomegalovirus_infection = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cytomegalovirus_infection): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when toxoplasomis_of_the_brain_1month
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET toxoplasomis_of_the_brain_1month = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (toxoplasomis_of_the_brain_1month): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET toxoplasomis_of_the_brain_1month = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (toxoplasomis_of_the_brain_1month): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when recto_vaginal_fitsula
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET recto_vaginal_fitsula = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (recto_vaginal_fitsula): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET recto_vaginal_fitsula = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (recto_vaginal_fitsula): #{patient_id}"

              end #voided
              
  ################################################################################################################################

            when pnuemocystis_pnuemonia
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET pnuemocystis_pnuemonia = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (pnuemocystis_pnuemonia): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET pnuemocystis_pnuemonia = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (pnuemocystis_pnuemonia): #{patient_id}"

              end #voided
            
  #################################################################################################################################
            
            when mod_wght_loss_less_thanequal_to_10_perc
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET moderate_weight_loss_less_than_or_equal_to_10_percent_unexpl = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (mod_wght_loss_less_thanequal_to_10_perc): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET moderate_weight_loss_less_than_or_equal_to_10_percent_unexpl = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (mod_wght_loss_less_thanequal_to_10_perc): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when seborrhoeic_dermatitis
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET seborrhoeic_dermatitis = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (seborrhoeic_dermatitis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET seborrhoeic_dermatitis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (seborrhoeic_dermatitis): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when hepatitis_b_or_c_infection
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET hepatitis_b_or_c_infection = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (hepatitis_b_or_c_infection): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET hepatitis_b_or_c_infection = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (hepatitis_b_or_c_infection): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when kaposis_sarcoma
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET kaposis_sarcoma = "#{answer}", kaposis_sarcoma_v_date = '#{Current_date}' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (kaposis_sarcoma): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET kaposis_sarcoma = NULL, kaposis_sarcoma_v_date = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (kaposis_sarcoma): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when non_typhoidal_salmonella_bacteraemia_recurrent
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET non_typhoidal_salmonella_bacteraemia_recurrent = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (non_typhoidal_salmonella_bacteraemia_recurrent): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET non_typhoidal_salmonella_bacteraemia_recurrent = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (non_typhoidal_salmonella_bacteraemia_recurrent): #{patient_id}"

              end #voided
              
  ################################################################################################################################

            when leishmaniasis_atypical_disseminated
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET leishmaniasis_atypical_disseminated = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (leishmaniasis_atypical_disseminated): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET leishmaniasis_atypical_disseminated = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (leishmaniasis_atypical_disseminated): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when cerebral_or_b_cell_non_hodgkin_lymphoma
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET cerebral_or_b_cell_non_hodgkin_lymphoma = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cerebral_or_b_cell_non_hodgkin_lymphoma): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET cerebral_or_b_cell_non_hodgkin_lymphoma = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cerebral_or_b_cell_non_hodgkin_lymphoma): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when invasive_cancer_of_cervix
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1 SET invasive_cancer_of_cervix = "#{answer}" WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (invasive_cancer_of_cervix): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1 SET invasive_cancer_of_cervix = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (invasive_cancer_of_cervix): #{patient_id}"

              end #voided
              
  ################################################################################################################################

            when cryptococcal_meningitis_or_other_eptb_cryptococcosis
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET cryptococcal_meningitis_or_other_eptb_cryptococcosis = "#{answer}",
                    cryptococcal_meningitis_or_other_eptb_cryptococcosis_v_date = '#{Current_date}',
                    cryptococcal_meningitis_or_other_eptb_cryptococcosis_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cryptococcal_meningitis_or_other_eptb_cryptococcosis): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET cryptococcal_meningitis_or_other_eptb_cryptococcosis = NULL,
                    cryptococcal_meningitis_or_other_eptb_cryptococcosis_v_date = NULL,
                    cryptococcal_meningitis_or_other_eptb_cryptococcosis_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cryptococcal_meningitis_or_other_eptb_cryptococcosis): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when candidiasis_of_oesophagus_trachea_bronchi_or_lungs
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET candidiasis_of_oesophagus_trachea_bronchi_or_lungs = "#{answer}",
                    candidiasis_of_oesophagus_trachea_bronchi_or_lungs_v_date = '#{Current_date}',
                    candidiasis_of_oesophagus_trachea_bronchi_or_lungs_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (candidiasis_of_oesophagus_trachea_bronchi_or_lungs): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET candidiasis_of_oesophagus_trachea_bronchi_or_lungs = NULL,
                    candidiasis_of_oesophagus_trachea_bronchi_or_lungs_v_date = NULL,
                    candidiasis_of_oesophagus_trachea_bronchi_or_lungs_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (candidiasis_of_oesophagus_trachea_bronchi_or_lungs): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when severe_unexplained_wasting_malnutrition
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET severe_unexplained_wasting_malnutrition = "#{answer}",
                    severe_unexplained_wasting_malnutrition_v_date = '#{Current_date}',
                    severe_unexplained_wasting_malnutrition_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (severe_unexplained_wasting_malnutrition): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET severe_unexplained_wasting_malnutrition = NULL,
                    severe_unexplained_wasting_malnutrition_v_date = NULL,
                    severe_unexplained_wasting_malnutrition_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (severe_unexplained_wasting_malnutrition): #{patient_id}"

              end #voided
              
  ################################################################################################################################

            when diarrhoea_chronic_less_1_month_unexplained
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET diarrhoea_chronic_less_1_month_unexplained = "#{answer}",
                    diarrhoea_chronic_less_1_month_unexplained_v_date = '#{Current_date}',
                    diarrhoea_chronic_less_1_month_unexplained_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (diarrhoea_chronic_less_1_month_unexplained): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET diarrhoea_chronic_less_1_month_unexplained = NULL,
                    diarrhoea_chronic_less_1_month_unexplained_v_date = NULL,
                    diarrhoea_chronic_less_1_month_unexplained_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (diarrhoea_chronic_less_1_month_unexplained): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when moderate_weight_loss_10_unexplained
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET moderate_weight_loss_10_unexplained = "#{answer}",
                    moderate_weight_loss_10_unexplained_v_date = '#{Current_date}',
                    moderate_weight_loss_10_unexplained_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (moderate_weight_loss_10_unexplained): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET acute_necrotizing_ulcerative_stomatitis_gingivitis_or_period = NULL,
                    acute_necrotizing_ulcerative_stomatitis_gingivitis_or_period_v_date = NULL,
                    acute_necrotizing_ulcerative_stomatitis_gingivitis_or_period_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (moderate_weight_loss_10_unexplained): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when acute_necrotizing_ulcerative_stomatitis_gingivitis_or_period
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET acute_necrotizing_ulcerative_stomatitis_gingivitis_or_period = "#{answer}",
                    acute_necrotizing_ulcerative_stomatitis_gingivitis_or_period_v_date = '#{Current_date}',
                    acute_necrotizing_ulcerative_stomatitis_gingivitis_or_period_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (acute_necrotizing_ulcerative_stomatitis_gingivitis_or_period): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET acute_necrotizing_ulcerative_stomatitis_gingivitis_or_period = NULL,
                    acute_necrotizing_ulcerative_stomatitis_gingivitis_or_period_v_date = NULL,
                    acute_necrotizing_ulcerative_stomatitis_gingivitis_or_period_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (acute_necrotizing_ulcerative_stomatitis_gingivitis_or_period): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when moderate_unexplained_wasting_malnutrition
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET moderate_unexplained_wasting_malnutrition = "#{answer}",
                    moderate_unexplained_wasting_malnutrition_v_date = '#{Current_date}',
                    moderate_unexplained_wasting_malnutrition_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (moderate_unexplained_wasting_malnutrition): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET moderate_unexplained_wasting_malnutrition = NULL,
                    moderate_unexplained_wasting_malnutrition_v_date = NULL,
                    moderate_unexplained_wasting_malnutrition_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (moderate_unexplained_wasting_malnutrition): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when diarrhoea_persistent_unexplained_14_days_or_more
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET diarrhoea_persistent_unexplained_14_days_or_more = "#{answer}",
                    diarrhoea_persistent_unexplained_14_days_or_more_v_date = '#{Current_date}',
                    diarrhoea_persistent_unexplained_14_days_or_more_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (diarrhoea_persistent_unexplained_14_days_or_more): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET diarrhoea_persistent_unexplained_14_days_or_more = NULL,
                    diarrhoea_persistent_unexplained_14_days_or_more_v_date = NULL,
                    diarrhoea_persistent_unexplained_14_days_or_more_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
              end #voided
              
  ##################################################################################################################################

            when acute_ulcerative_mouth_infections
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET acute_ulcerative_mouth_infections = "#{answer}",
                    acute_ulcerative_mouth_infections_v_date = '#{Current_date}',
                    acute_ulcerative_mouth_infections_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (acute_ulcerative_mouth_infections): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET acute_ulcerative_mouth_infections = NULL,
                    acute_ulcerative_mouth_infections_v_date = NULL,
                    acute_ulcerative_mouth_infections_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (acute_ulcerative_mouth_infections): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when anaemia_unexplained_8_g_dl
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET anaemia_unexplained_8_g_dl = "#{answer}",
                    anaemia_unexplained_8_g_dl_v_date = '#{Current_date}',
                    anaemia_unexplained_8_g_dl_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (anaemia_unexplained_8_g_dl): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET anaemia_unexplained_8_g_dl = NULL,
                    anaemia_unexplained_8_g_dl_v_date = NULL,
                    anaemia_unexplained_8_g_dl_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (anaemia_unexplained_8_g_dl): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when atypical_mycobacteriosis_disseminated_or_lung
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET atypical_mycobacteriosis_disseminated_or_lung = "#{answer}",
                    atypical_mycobacteriosis_disseminated_or_lung_v_date = '#{Current_date}',
                    atypical_mycobacteriosis_disseminated_or_lung_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (atypical_mycobacteriosis_disseminated_or_lung): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET atypical_mycobacteriosis_disseminated_or_lung = NULL,
                    atypical_mycobacteriosis_disseminated_or_lung_v_date = NULL,
                    atypical_mycobacteriosis_disseminated_or_lung_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (atypical_mycobacteriosis_disseminated_or_lung): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when bacterial_infections_sev_recurrent_excluding_pneumonia
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET bacterial_infections_sev_recurrent_excluding_pneumonia = "#{answer}",
                    bacterial_infections_sev_recurrent_excluding_pneumonia_v_date = '#{Current_date}',
                    bacterial_infections_sev_recurrent_excluding_pneumonia_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (atypical_mycobacteriosis_disseminated_or_lung): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET bacterial_infections_sev_recurrent_excluding_pneumonia = NULL,
                    bacterial_infections_sev_recurrent_excluding_pneumonia_v_date = NULL,
                    bacterial_infections_sev_recurrent_excluding_pneumonia_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (atypical_mycobacteriosis_disseminated_or_lung): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when cancer_cervix
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET cancer_cervix = "#{answer}",
                    cancer_cervix_v_date = '#{Current_date}',
                    cancer_cervix_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cancer_cervix): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET cancer_cervix = NULL,
                    cancer_cervix_v_date = NULL,
                    cancer_cervix_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cancer_cervix): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when chronic_herpes_simplex_infection_genital
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET chronic_herpes_simplex_infection_genital = "#{answer}",
                    chronic_herpes_simplex_infection_genital_v_date = '#{Current_date}',
                    chronic_herpes_simplex_infection_genital_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cancer_cervix): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when cryptosporidiosis_chronic_with_diarrhoea
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET cryptosporidiosis_chronic_with_diarrhoea = "#{answer}",
                    cryptosporidiosis_chronic_with_diarrhoea_v_date = '#{Current_date}',
                    cryptosporidiosis_chronic_with_diarrhoea_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cryptosporidiosis_chronic_with_diarrhoea): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET cryptosporidiosis_chronic_with_diarrhoea = NULL,
                    cryptosporidiosis_chronic_with_diarrhoea_v_date = NULL,
                    cryptosporidiosis_chronic_with_diarrhoea_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cryptosporidiosis_chronic_with_diarrhoea): #{patient_id}"

              end #voided
              
  ################################################################################################################################
            
            when cytomegalovirus_infection_retinitis_or_other_organ
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET cytomegalovirus_infection_retinitis_or_other_organ = "#{answer}",
                    cytomegalovirus_infection_retinitis_or_other_organ_v_date = '#{Current_date}',
                    cytomegalovirus_infection_retinitis_or_other_organ_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cytomegalovirus_infection_retinitis_or_other_organ): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET cytomegalovirus_infection_retinitis_or_other_organ = NULL,
                    cytomegalovirus_infection_retinitis_or_other_organ_v_date = NULL,
                    cytomegalovirus_infection_retinitis_or_other_organ_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cytomegalovirus_infection_retinitis_or_other_organ): #{patient_id}"

              end #voided
              
  ###############################################################################################################################
            
            when cytomegalovirus_of_an_organ_other_than_liver
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET cytomegalovirus_of_an_organ_other_than_liver = "#{answer}",
                    cytomegalovirus_of_an_organ_other_than_liver_v_date = '#{Current_date}',
                    cytomegalovirus_of_an_organ_other_than_liver_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cytomegalovirus_of_an_organ_other_than_liver): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET cytomegalovirus_of_an_organ_other_than_liver = NULL,
                    cytomegalovirus_of_an_organ_other_than_liver_v_date = NULL,
                    cytomegalovirus_of_an_organ_other_than_liver_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cytomegalovirus_of_an_organ_other_than_liver): #{patient_id}"

              end #voided
              
  ################################################################################################################################

            when fungal_nail_infections
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                UPDATE flat_table1
                SET fungal_nail_infections = "#{answer}",
                    fungal_nail_infections_v_date = '#{Current_date}',
                    fungal_nail_infections_enc_id = "#{encounter_id}"
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (fungal_nail_infections): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                SET fungal_nail_infections = NULL,
                    fungal_nail_infections_v_date = NULL,
                    fungal_nail_infections_enc_id = NULL
                WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (fungal_nail_infections): #{patient_id}"

              end #voided
              
  ################################################################################################################################

            when herpes_simplex_infection_mucocutaneous_visceral
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET herpes_simplex_infection_mucocutaneous_visceral = "#{answer}",
                      herpes_simplex_infection_mucocutaneous_visceral_v_date = '#{Current_date}',
                      herpes_simplex_infection_mucocutaneous_visceral_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (herpes_simplex_infection_mucocutaneous_visceral): #{patient_id}"

              else
                Connection.execute <<EOF
                UPDATE flat_table1
                  SET herpes_simplex_infection_mucocutaneous_visceral = NULL,
                      herpes_simplex_infection_mucocutaneous_visceral_v_date = NULL,
                      herpes_simplex_infection_mucocutaneous_visceral_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
              end #voided
              
  ###############################################################################################################################

            when hiv_associated_cardiomyopathy
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET hiv_associated_cardiomyopathy = "#{answer}",
                      hiv_associated_cardiomyopathy_v_date = '#{Current_date}',
                      hiv_associated_cardiomyopathy_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (hiv_associated_cardiomyopathy): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET hiv_associated_cardiomyopathy = NULL,
                      hiv_associated_cardiomyopathy_v_date = NULL,
                      hiv_associated_cardiomyopathy_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (hiv_associated_cardiomyopathy): #{patient_id}"

              end #voided
              
  ##################################################################################################################################
            
            when hiv_associated_nephropathy
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET hiv_associated_nephropathy = "#{answer}",
                      hiv_associated_nephropathy_v_date = '#{Current_date}',
                      hiv_associated_nephropathy_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (hiv_associated_nephropathy): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET hiv_associated_nephropathy = NULL,
                      hiv_associated_nephropathy_v_date = NULL,
                      hiv_associated_nephropathy_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (hiv_associated_nephropathy): #{patient_id}"

              end #voided
              
  ###################################################################################################################################
            
            when invasive_cancer_cervix
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET invasive_cancer_cervix = "#{answer}",
                      invasive_cancer_cervix_v_date = '#{Current_date}',
                      invasive_cancer_cervix_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (invasive_cancer_cervix): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET invasive_cancer_cervix = NULL,
                      invasive_cancer_cervix_v_date = NULL,
                      invasive_cancer_cervix_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (invasive_cancer_cervix): #{patient_id}"

              end #voided
              
  ####################################################################################################################################

            when isosporiasis_1_month
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET isosporiasis_1_month = "#{answer}",
                      isosporiasis_1_month_v_date = '#{Current_date}',
                      isosporiasis_1_month_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (isosporiasis_1_month): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET isosporiasis_1_month = NULL,
                      isosporiasis_1_month_v_date = NULL,
                      isosporiasis_1_month_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (isosporiasis_1_month): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when leishmaniasis_atypical_disseminated
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET leishmaniasis_atypical_disseminated = "#{answer}",
                      leishmaniasis_atypical_disseminated_v_date = '#{Current_date}',
                      leishmaniasis_atypical_disseminated_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (leishmaniasis_atypical_disseminated): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET leishmaniasis_atypical_disseminated = NULL,
                      leishmaniasis_atypical_disseminated_v_date = NULL,
                      leishmaniasis_atypical_disseminated_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (leishmaniasis_atypical_disseminated): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when minor_mucocutaneous_manifestations_seborrheic_dermatitis
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET minor_mucocutaneous_manifestations_seborrheic_dermatitis = "#{answer}",
                      minor_mucocutaneous_manifestations_seborrheic_dermatitis_v_date = '#{Current_date}',
                      minor_mucocutaneous_manifestations_seborrheic_dermatitis_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (minor_mucocutaneous_manifestations_seborrheic_dermatitis): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET minor_mucocutaneous_manifestations_seborrheic_dermatitis = NULL,
                      minor_mucocutaneous_manifestations_seborrheic_dermatitis_v_date = NULL,
                      minor_mucocutaneous_manifestations_seborrheic_dermatitis_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (minor_mucocutaneous_manifestations_seborrheic_dermatitis): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when cd4_percentage_available
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET cd4_percentage_available = @"#{answer}",
                      cd4_percentage_available_v_date = '#{Current_date}',
                      cd4_percentage_available_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cd4_percentage_available): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET cd4_percentage_available = NULL,
                      cd4_percentage_available_v_date = NULL,
                      cd4_percentage_available_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (cd4_percentage_available): #{patient_id}"

              end #voided
              
  ################################################################################################################################

            when moderate_unexplained_malnutrition
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET moderate_unexplained_malnutrition = "#{answer}",
                      moderate_unexplained_malnutrition_v_date = '#{Current_date}',
                      moderate_unexplained_malnutrition_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (moderate_unexplained_malnutrition): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET moderate_unexplained_malnutrition = NULL,
                      moderate_unexplained_malnutrition_v_date = NULL,
                      moderate_unexplained_malnutrition_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (moderate_unexplained_malnutrition): #{patient_id}"

              end #voided

  #################################################################################################################################

            when molluscum_contagiosum_extensive
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET molluscum_contagiosum_extensive = "#{answer}",
                      molluscum_contagiosum_extensive_v_date = '#{Current_date}',
                      molluscum_contagiosum_extensive_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (molluscum_contagiosum_extensive): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET molluscum_contagiosum_extensive = NULL,
                      molluscum_contagiosum_extensive_v_date = NULL,
                      molluscum_contagiosum_extensive_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (molluscum_contagiosum_extensive): #{patient_id}"

              end #voided
              
  ################################################################################################################################

            when non_typhoidal_salmonella_bacteraemia_recurrent
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET non_typhoidal_salmonella_bacteraemia_recurrent = "#{answer}",
                      non_typhoidal_salmonella_bacteraemia_recurrent_v_date = '#{Current_date}',
                      non_typhoidal_salmonella_bacteraemia_recurrent_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (non_typhoidal_salmonella_bacteraemia_recurrent): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET non_typhoidal_salmonella_bacteraemia_recurrent = NULL,
                      non_typhoidal_salmonella_bacteraemia_recurrent_v_date = NULL,
                      non_typhoidal_salmonella_bacteraemia_recurrent_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (non_typhoidal_salmonella_bacteraemia_recurrent): #{patient_id}"

              end #voided
              
  ################################################################################################################################

            when oral_candidiasis_from_age_2_months
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET oral_candidiasis_from_age_2_months = "#{answer}",
                      oral_candidiasis_from_age_2_months_v_date = '#{Current_date}',
                      oral_candidiasis_from_age_2_months_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (oral_candidiasis_from_age_2_months): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET oral_candidiasis_from_age_2_months = NULL,
                      oral_candidiasis_from_age_2_months_v_date = NULL,
                      oral_candidiasis_from_age_2_months_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (oral_candidiasis_from_age_2_months): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when oral_thrush
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET oral_thrush = "#{answer}",
                      oral_thrush_v_date = '#{Current_date}',
                      oral_thrush_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (oral_thrush): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET oral_thrush = NULL,
                      oral_thrush_v_date = NULL,
                      oral_thrush_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (oral_thrush): #{patient_id}"

              end #voided
              
  ##############################################################################################################################

            when perform_extended_staging
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET perform_extended_staging = "#{answer}",
                      perform_extended_staging_v_date = '#{Current_date}',
                      perform_extended_staging_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (perform_extended_staging): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET perform_extended_staging = NULL,
                      perform_extended_staging_v_date = NULL,
                      perform_extended_staging_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (perform_extended_staging): #{patient_id}"

              end #voided
              
  ###############################################################################################################################

            when pneumocystis_carinii_pneumonia
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET pneumocystis_carinii_pneumonia = "#{answer}",
                      pneumocystis_carinii_pneumonia_v_date = '#{Current_date}',
                      pneumocystis_carinii_pneumonia_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (pneumocystis_carinii_pneumonia): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET pneumocystis_carinii_pneumonia = NULL,
                      pneumocystis_carinii_pneumonia_v_date = NULL,
                      pneumocystis_carinii_pneumonia_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (pneumocystis_carinii_pneumonia): #{patient_id}"

              end #voided
              
  ################################################################################################################################

            when pneumonia_severe
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET pneumonia_severe = "#{answer}",
                      pneumonia_severe_v_date = '#{Current_date}',
                      pneumonia_severe_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (pneumonia_severe): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET pneumonia_severe = NULL,
                      pneumonia_severe_v_date = NULL,
                      pneumonia_severe_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (pneumonia_severe): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when recurrent_bacteraemia_or_sepsis_with_nts
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET recurrent_bacteraemia_or_sepsis_with_nts = "#{answer}",
                      recurrent_bacteraemia_or_sepsis_with_nts_v_date = '#{Current_date}',
                      recurrent_bacteraemia_or_sepsis_with_nts_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (recurrent_bacteraemia_or_sepsis_with_nts): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET recurrent_bacteraemia_or_sepsis_with_nts = NULL,
                      recurrent_bacteraemia_or_sepsis_with_nts_v_date = NULL,
                      recurrent_bacteraemia_or_sepsis_with_nts_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (recurrent_bacteraemia_or_sepsis_with_nts): #{patient_id}"

              end #voided
              
  ###############################################################################################################################

            when recurrent_severe_presumed_pneumonia
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET recurrent_severe_presumed_pneumonia = "#{answer}",
                      recurrent_severe_presumed_pneumonia_v_date = '#{Current_date}',
                      recurrent_severe_presumed_pneumonia_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (recurrent_severe_presumed_pneumonia): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET recurrent_severe_presumed_pneumonia = NULL,
                      recurrent_severe_presumed_pneumonia_v_date = NULL,
                      recurrent_severe_presumed_pneumonia_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (recurrent_severe_presumed_pneumonia): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when recurrent_upper_respiratory_tract_bac_sinusitis
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET recurrent_upper_respiratory_tract_bac_sinusitis = "#{answer}",
                      recurrent_upper_respiratory_tract_bac_sinusitis_v_date = '#{Current_date}',
                      recurrent_upper_respiratory_tract_bac_sinusitis_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (recurrent_upper_respiratory_tract_bac_sinusitis): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET recurrent_upper_respiratory_tract_bac_sinusitis = NULL,
                      recurrent_upper_respiratory_tract_bac_sinusitis_v_date = NULL,
                      recurrent_upper_respiratory_tract_bac_sinusitis_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (recurrent_upper_respiratory_tract_bac_sinusitis): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when seborrhoeic_dermatitis
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET seborrhoeic_dermatitis = "#{answer}",
                      seborrhoeic_dermatitis_v_date = '#{Current_date}',
                      seborrhoeic_dermatitis_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (seborrhoeic_dermatitis): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET seborrhoeic_dermatitis = NULL,
                      seborrhoeic_dermatitis_v_date = NULL,
                      seborrhoeic_dermatitis_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (seborrhoeic_dermatitis): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when sepsis_severe
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET sepsis_severe = "#{answer}",
                      sepsis_severe_v_date = '#{Current_date}',
                      sepsis_severe_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (sepsis_severe): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET sepsis_severe = NULL,
                      sepsis_severe_v_date = NULL,
                      sepsis_severe_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (sepsis_severe): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when tb_lymphadenopathy
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET tb_lymphadenopathy = "#{answer}",
                      tb_lymphadenopathy_v_date = '#{Current_date}',
                      tb_lymphadenopathy_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (tb_lymphadenopathy): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET tb_lymphadenopathy = NULL,
                      tb_lymphadenopathy_v_date = NULL,
                      tb_lymphadenopathy_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (tb_lymphadenopathy): #{patient_id}"

              end #voided
              
  ##################################################################################################################################

            when unexplained_anaemia_neutropenia_or_thrombocytopenia
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET unexplained_anaemia_neutropenia_or_thrombocytopenia = "#{answer}",
                      unexplained_anaemia_neutropenia_or_thrombocytopenia_v_date = '#{Current_date}',
                      unexplained_anaemia_neutropenia_or_thrombocytopenia_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (unexplained_anaemia_neutropenia_or_thrombocytopenia): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET unexplained_anaemia_neutropenia_or_thrombocytopenia = NULL,
                      unexplained_anaemia_neutropenia_or_thrombocytopenia_v_date = NULL,
                      unexplained_anaemia_neutropenia_or_thrombocytopenia_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (unexplained_anaemia_neutropenia_or_thrombocytopenia): #{patient_id}"

              end #voided
              
  #################################################################################################################################

            when visceral_leishmaniasis
              if voided.blank?
                answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                  WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ")
                answer = answer_record['name']

                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET visceral_leishmaniasis = "#{answer}",
                      visceral_leishmaniasis_v_date = '#{Current_date}',
                      visceral_leishmaniasis_enc_id = "#{encounter_id}"
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (visceral_leishmaniasis): #{patient_id}"

              else
                Connection.execute <<EOF
                  UPDATE flat_table1
                  SET visceral_leishmaniasis = NULL,
                      visceral_leishmaniasis_v_date = NULL,
                      visceral_leishmaniasis_enc_id = NULL
                  WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (visceral_leishmaniasis): #{patient_id}"

              end #voided
              
  ################################################################################################################################

            when who_crit_stage
              answer_record = Connection.select_one("SELECT name from concept_name LEFT OUTER JOIN concept ON concept.concept_id = concept_name.concept_id
                             WHERE  concept.concept_id = '#{value_coded}' AND voided = 0 AND retired = 0 ");
              answer = answer_record['name']

              if voided.blank?
                if (answer == 'Acute necrotizing ulcerative stomatitis, gingivitis or periodontitis')
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET acute_necrotizing_ulcerative_gingivitis = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                puts ".......... Updating record into flat_table1 (who_crit_stage: acute_necrotizing_ulcerative_gingivitis): #{patient_id}"

                end #answer 'Acute necrotizing ulcerative stomatitis, gingivitis or periodontitis'

                if (answer == 'Anaemia, unexplained < 8 g/dl')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET anaemia_unexplained_8_g_dl = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: anaemia_unexplained_8_g_dl): #{patient_id}"

                end #answer == 'Anaemia, unexplained < 8 g/dl'

                if (answer == 'Angular cheilitis')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET angular_chelitis = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: angular_chelitis): #{patient_id}"

                end #'Angular cheilitis'

                if (answer == 'Asymptomatic HIV infection')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET asymptomatic = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: asymptomatic): #{patient_id}"

                end #Asymptomatic HIV infection

                if (answer == 'Bacterial infections, severe recurrent  (empyema, pyomyositis, meningitis, bone/joint infections but EXCLUDING pneumonia)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET bacterial_infections_severe_recurrent = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: bacterial_infections_severe_recurrent): #{patient_id}"

                end #bacterial_infections_severe_recurrent

                if (answer == 'Bacterial pneumonia, severe recurrent')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET bacterial_pnuemonia = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: bacterial_pnuemonia): #{patient_id}"

                end #bacterial_pnuemonia

                if (answer == 'Candidiasis of oseophagus, trachea and bronchi or lungs')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET candidiasis_of_oesophagus = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: candidiasis_of_oesophagus): #{patient_id}"

                end #candidiasis_of_oesophagus

                if (answer == 'Cerebral or B-cell non Hodgkin lymphoma')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET cerebral_non_hodgkin_lymphoma = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: cerebral_non_hodgkin_lymphoma): #{patient_id}"

                end #cerebral_non_hodgkin_lymphoma

                if (answer == 'Chronic herpes simplex infection (orolabial, gential / anorectal >1 month or visceral at any site)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET chronic_herpes_simplex_infection = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: chronic_herpes_simplex_infection): #{patient_id}"

                end #chronic_herpes_simplex_infection

                if (answer == 'Cryptococcal meningitis or other extrapulmonary cryptococcosis')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET cryptococcal_meningitis = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: cryptococcal_meningitis): #{patient_id}"

                end #cryptococcal_meningitis

                if (answer == 'Cryptosporidiosis, chronic with diarroea')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET cryptosporidiosis = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: cryptosporidiosis): #{patient_id}"

                end #cryptosporidiosis

                if (answer == 'Cytomegalovirus infection (retinitis or infection or other organs)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET cytomegalovirus_infection = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: cytomegalovirus_infection): #{patient_id}"

                end #cytomegalovirus_infection

                if (answer == 'Diarrhoea, chronic (>1 month) unexplained')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET diarhoea = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: diarhoea): #{patient_id}"

                end #diarhoea

                if (answer == 'Diarrhoea, persistent unexplained (14 days or more)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET diarhoea = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: diarhoea): #{patient_id}"

                end #diarhoea

                if (answer == 'Disseminated mycosis (coccidiomycosis or histoplasmosis)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET disseminated_mycosis = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: disseminated_mycosis): #{patient_id}"

                end #disseminated_mycosis

                if (answer == 'Disseminated non-tuberculosis mycobacterial infection')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET disseminated_non_tuberculosis_mycobacterial_infection = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: disseminated_non_tuberculosis_mycobacterial_infection): #{patient_id}"

                end #disseminated_non_tuberculosis_mycobacterial_infection

                if (answer == 'Extrapulmonary tuberculosis (EPTB)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET extrapulmonary_tuberculosis = 'Yes', extrapulmonary_tuberculosis_v_date = '#{Current_date}' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: extrapulmonary_tuberculosis): #{patient_id}"

                end #extrapulmonary_tuberculosis

                if (answer == 'Fever, persistent unexplained, intermittent or constant, >1 month')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET fever_persistent_unexplained = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: fever_persistent_unexplained): #{patient_id}"

                end #fever_persistent_unexplained

                if (answer == 'Hepatosplenomegaly, persistent unexplained')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET hepatosplenomegaly_unexplained = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: hepatosplenomegaly_unexplained): #{patient_id}"

                end #hepatosplenomegaly_unexplained

                if (answer == 'Herpes zoster')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET herpes_zoster = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: herpes_zoster): #{patient_id}"

                end #herpes_zoster

                if (answer == 'HIV encephalopathy')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET hiv_encephalopathy = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: hiv_encephalopathy): #{patient_id}"

                end #hiv_encephalopathy

                if (answer == 'HIV wasting syndrome (severe weight loss + persistent fever or severe weight loss + chronic diarrhoea)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET severe_weight_loss = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: severe_weight_loss): #{patient_id}"

                end #severe_weight_loss

                if (answer == 'Invasive cancer of cervix')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET invasive_cancer_of_cervix = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: invasive_cancer_of_cervix): #{patient_id}"

                end #invasive_cancer_of_cervix

                if (answer == 'Isosporiasis >1 month')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET isosporiasis = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: isosporiasis): #{patient_id}"

                end #isosporiasis

                if (answer == 'Kaposis sarcoma')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET kaposis_sarcoma = 'Yes', kaposis_sarcoma_v_date = '#{Current_date}' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: kaposis_sarcoma): #{patient_id}"

                end #kaposis_sarcoma

                if (answer == 'Moderate unexplained wasting/malnutrition not responding to treatment (weight-for-height/ -age 70-79% or muac 11-12 cm)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET moderate_weight_loss_less_than_or_equal_to_10_percent_unexpl = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: moderate_weight_loss_less_than_or_equal_to_10_percent_unexpl): #{patient_id}"

                end #moderate_weight_loss_less_than_or_equal_to_10_percent_unexpl

                if (answer == 'Moderate weight loss less than or equal to 10 percent, unexplained')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET moderate_weight_loss_less_than_or_equal_to_10_percent_unexpl = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: moderate_weight_loss_less_than_or_equal_to_10_percent_unexpl): #{patient_id}"

                end #moderate_weight_loss_less_than_or_equal_to_10_percent_unexpl

                if (answer == 'Molluscum contagiosum, extensive')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET molluscumm_contagiosum = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: molluscumm_contagiosum): #{patient_id}"

                end #molluscumm_contagiosum

                if (answer == 'Neutropaenia, unexplained < 500 /mm(cubed)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET neutropaenia = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: neutropaenia): #{patient_id}"

                end #neutropaenia

                if (answer == 'Oral candidiasis')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET oral_candidiasis = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: oral_candidiasis): #{patient_id}"

                end #oral_candidiasis

                if (answer == 'Oral candidiasis (from age 2 months)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET oral_candidiasis = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: oral_candidiasis): #{patient_id}"

                end #oral_candidiasis

                if (answer == 'Oral hairy leukoplakia')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET oral_hairy_leukoplakia = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: oral_hairy_leukoplakia): #{patient_id}"

                end #oral_hairy_leukoplakia

                if (answer == 'Oral ulcerations, recurrent')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET oral_hairy_leukoplakia = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: oral_hairy_leukoplakia): #{patient_id}"

                end #oral_hairy_leukoplakia

                if (answer == 'Papular pruritic eruptions / Fungal nail infections')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET papular_pruritic_eruptions = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: papular_pruritic_eruptions): #{patient_id}"

                end #papular_pruritic_eruptions

                if (answer == 'Parotid enlargement, persistent unexplained')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET parotid_enlargement_persistent_unexplained = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: parotid_enlargement_persistent_unexplained): #{patient_id}"

                end #parotid_enlargement_persistent_unexplained

                if (answer == 'Persistent generalized lymphadenopathy')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET persistent_generalized_lymphadenopathy = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: persistent_generalized_lymphadenopathy): #{patient_id}"

                end #persistent_generalized_lymphadenopathy

                if (answer == 'Pneumocystis pneumonia')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET pnuemocystis_pnuemonia = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: pnuemocystis_pnuemonia): #{patient_id}"

                end #pnuemocystis_pnuemonia

                if (answer == 'Progressive multifocal leukoencephalopathy')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET progressive_multifocal_leukoencephalopathy = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: progressive_multifocal_leukoencephalopathy): #{patient_id}"

                end #progressive_multifocal_leukoencephalopathy

                if (answer == 'Pulmonary tuberculosis (current)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET pulmonary_tuberculosis_last_2_years = 'Yes', pulmonary_tuberculosis_last_2_years_v_date = '#{Current_date}' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: pulmonary_tuberculosis_last_2_years): #{patient_id}"

                end #pulmonary_tuberculosis_last_2_years

                if (answer == 'Respiratory tract infections, recurrent (sinusitis, tonsilitus, otitis media, pharyngitis)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET respiratory_tract_infections_recurrent = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: respiratory_tract_infections_recurrent): #{patient_id}"

                end #respiratory_tract_infections_recurrent

                if (answer == 'Seborrhoeic dermatitis')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET seborrhoeic_dermatitis = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: seborrhoeic_dermatitis): #{patient_id}"

                end #seborrhoeic_dermatitis

                if (answer == 'Severe bacterial infections (pneumonia, empyema, pyomyositis, bone/joint, meningitis, bacteraemia)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET severe_bacterial_infection = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: severe_bacterial_infection): #{patient_id}"

                end #severe_bacterial_infection

                if (answer == 'Severe unexplained wasting or malnutrition not responding to treatment (weight-for-height/ -age <70% or MUAC less than 11cm or oedema)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET severe_unexplained_wasting_malnutrition = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: severe_unexplained_wasting_malnutrition): #{patient_id}"

                end #severe_unexplained_wasting_malnutrition

                if (answer == 'Severe weight loss >10% and/or BMI <18.5kg/m^2, unexplained')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET severe_weight_loss = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: severe_weight_loss): #{patient_id}"

                end #severe_weight_loss

                if (answer == 'Symptomatic HIV-associated nephropathy or cardiomyopathy')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET symptomatic_hiv_associated_nephropathy = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: symptomatic_hiv_associated_nephropathy): #{patient_id}"

                end #symptomatic_hiv_associated_nephropathy

                if (answer == 'Thrombocytopaenia, chronic < 50,000 /mm(cubed)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET thrombocytopaenia_chronic = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: thrombocytopaenia_chronic): #{patient_id}"

                end #thrombocytopaenia_chronic

                if (answer == 'Toxoplasmosis of the brain')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET toxoplasmosis_of_the_brain  = 'Yes' WHERE flat_table1.patient_id= in_patient_id;
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: toxoplasmosis_of_the_brain): #{patient_id}"

                end #toxoplasmosis_of_the_brain

                if (answer == 'Tuberculosis (PTB or EPTB) within the last 2 years')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET pulmonary_tuberculosis_last_2_years = 'Yes', pulmonary_tuberculosis_last_2_years_v_date = '#{Current_date}' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: pulmonary_tuberculosis_last_2_years): #{patient_id}"

                end #pulmonary_tuberculosis_last_2_years

                if (answer == 'Unspecified stage I condition')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET unspecified_stage_1_cond = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: unspecified_stage_1_cond): #{patient_id}"

                end #unspecified_stage_1_cond

                if (answer == 'Unspecified stage II condition')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET unspecified_stage2_condition = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: unspecified_stage2_condition): #{patient_id}"

                end #unspecified_stage2_condition

                if (answer == 'Unspecified stage III condition')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET unspecified_stage3_condition = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: unspecified_stage3_condition): #{patient_id}"

                end #unspecified_stage3_condition

                if (answer == 'Unspecified stage IV condition')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET unspecified_stage_4_condition = 'Yes' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: unspecified_stage_4_condition): #{patient_id}"

                end #unspecified_stage_4_condition

              else

                if (answer == 'Acute necrotizing ulcerative stomatitis, gingivitis or periodontitis')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET acute_necrotizing_ulcerative_gingivitis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: acute_necrotizing_ulcerative_gingivitis): #{patient_id}"

                end #acute_necrotizing_ulcerative_gingivitis

                if (answer == 'Anaemia, unexplained < 8 g/dl')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET aneamia = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: aneamia): #{patient_id}"

                end #aneamia

                if (answer == 'Angular cheilitis')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET angular_chelitis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF               
                  puts ".......... Updating record into flat_table1 (who_crit_stage: angular_chelitis): #{patient_id}"

                end #angular_chelitis

                if (answer == 'Asymptomatic HIV infection')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET asymptomatic = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: asymptomatic): #{patient_id}"

                end #asymptomatic

                if (answer == 'Bacterial infections, severe recurrent  (empyema, pyomyositis, meningitis, bone/joint infections but EXCLUDING pneumonia)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET bacterial_infections_severe_recurrent = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: bacterial_infections_severe_recurrent): #{patient_id}"

                end #bacterial_infections_severe_recurrent

                if (answer == 'Bacterial pneumonia, severe recurrent')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET bacterial_pnuemonia = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: bacterial_pnuemonia): #{patient_id}"

                end #bacterial_pnuemonia

                if (answer == 'Candidiasis of oseophagus, trachea and bronchi or lungs')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET candidiasis_of_oesophagus = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: candidiasis_of_oesophagus): #{patient_id}"

                end #candidiasis_of_oesophagus

                if (answer == 'Cerebral or B-cell non Hodgkin lymphoma')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET cerebral_non_hodgkin_lymphoma = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: cerebral_non_hodgkin_lymphoma): #{patient_id}"

                end #cerebral_non_hodgkin_lymphoma

                if (answer == 'Chronic herpes simplex infection (orolabial, gential / anorectal >1 month or visceral at any site)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET chronic_herpes_simplex_infection = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: chronic_herpes_simplex_infection): #{patient_id}"

                end #chronic_herpes_simplex_infection

                if (answer == 'Cryptococcal meningitis or other extrapulmonary cryptococcosis')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET cryptococcal_meningitis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: cryptococcal_meningitis): #{patient_id}"

                end #cryptococcal_meningitis

                if (answer == 'Cryptosporidiosis, chronic with diarroea')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET cryptosporidiosis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: cryptosporidiosis): #{patient_id}"

                end #cryptosporidiosis

                if (answer == 'Cytomegalovirus infection (retinitis or infection or other organs)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET cytomegalovirus_infection = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: cytomegalovirus_infection): #{patient_id}"

                end #cytomegalovirus_infection

                if (answer == 'Diarrhoea, chronic (>1 month) unexplained')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET diarrhoea_chronic_less_1_month_unexplained = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: diarrhoea_chronic_less_1_month_unexplained): #{patient_id}"

                end #diarrhoea_chronic_less_1_month_unexplained

                if (answer == 'Diarrhoea, persistent unexplained (14 days or more)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET diarrhoea_persistent_unexplained_14_days_or_more = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: diarrhoea_persistent_unexplained_14_days_or_more): #{patient_id}"

                end #diarrhoea_persistent_unexplained_14_days_or_more

                if (answer == 'Disseminated mycosis (coccidiomycosis or histoplasmosis)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET disseminated_mycosis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: disseminated_mycosis): #{patient_id}"

                end #disseminated_mycosis

                if (answer == 'Disseminated non-tuberculosis mycobacterial infection')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET disseminated_non_tuberculosis_mycobacterial_infection = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: disseminated_non_tuberculosis_mycobacterial_infection): #{patient_id}"

                end #disseminated_non_tuberculosis_mycobacterial_infection

                if (answer == 'Extrapulmonary tuberculosis (EPTB)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET extrapulmonary_tuberculosis = NULL, extrapulmonary_tuberculosis_v_date = '#{Current_date}' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: extrapulmonary_tuberculosis): #{patient_id}"

                end #extrapulmonary_tuberculosis

                if (answer == 'Fever, persistent unexplained, intermittent or constant, >1 month')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET fever_persistent_unexplained = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: fever_persistent_unexplained): #{patient_id}"

                end #fever_persistent_unexplained

                if (answer == 'Hepatosplenomegaly, persistent unexplained')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET hepatosplenomegaly_unexplained = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: hepatosplenomegaly_unexplained): #{patient_id}"

                end #hepatosplenomegaly_unexplained

                if (answer == 'Herpes zoster')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET herpes_zoster = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: herpes_zoster): #{patient_id}"

                end #herpes_zoster

                if (answer == 'HIV encephalopathy')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET hiv_encephalopathy = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: hiv_encephalopathy): #{patient_id}"

                end #hiv_encephalopathy

                if (answer == 'HIV wasting syndrome (severe weight loss + persistent fever or severe weight loss + chronic diarrhoea)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET severe_weight_loss = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: severe_weight_loss): #{patient_id}"

                end #severe_weight_loss

                if (answer == 'Invasive cancer of cervix')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET invasive_cancer_of_cervix = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: invasive_cancer_of_cervix): #{patient_id}"

                end #invasive_cancer_of_cervix

                if (answer == 'Isosporiasis >1 month')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET isosporiasis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: isosporiasis): #{patient_id}"

                end #isosporiasis

                if (answer == 'Kaposis sarcoma')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET kaposis_sarcoma = NULL, kaposis_sarcoma_v_date = in_visit_date WHERE flat_table1.patient_id= in_patient_id;
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: kaposis_sarcoma): #{patient_id}"

                end #kaposis_sarcoma

                if (answer == 'Moderate unexplained wasting/malnutrition not responding to treatment (weight-for-height/ -age 70-79% or muac 11-12 cm)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET moderate_weight_loss_less_than_or_equal_to_10_percent_unexpl = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: moderate_weight_loss_less_than_or_equal_to_10_percent_unexpl): #{patient_id}"

                end #moderate_weight_loss_less_than_or_equal_to_10_percent_unexpl

                if (answer == 'Moderate weight loss less than or equal to 10 percent, unexplained')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET moderate_weight_loss_less_than_or_equal_to_10_percent_unexpl = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: moderate_weight_loss_less_than_or_equal_to_10_percent_unexpl): #{patient_id}"

                end #moderate_weight_loss_less_than_or_equal_to_10_percent_unexpl

                if (answer == 'Molluscum contagiosum, extensive')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET molluscumm_contagiosum = NULL WHERE flat_table1.patient_id= in_patient_id;
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: molluscumm_contagiosum): #{patient_id}"

                end #molluscumm_contagiosum

                if (answer == 'Neutropaenia, unexplained < 500 /mm(cubed)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET neutropaenia = NULL WHERE flat_table1.patient_id= in_patient_id;
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: neutropaenia): #{patient_id}"

                end #neutropaenia

                if (answer == 'Oral candidiasis')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET oral_candidiasis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: oral_candidiasis): #{patient_id}"

                end #oral_candidiasis

                if (answer == 'Oral candidiasis (from age 2 months)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET oral_candidiasis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: oral_candidiasis): #{patient_id}"

                end #oral_candidiasis

                if (answer == 'Oral hairy leukoplakia')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET oral_hairy_leukoplakia = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: oral_hairy_leukoplakia): #{patient_id}"

                end #oral_hairy_leukoplakia

                if (answer == 'Oral ulcerations, recurrent')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET oral_hairy_leukoplakia = NULL WHERE flat_table1.patient_id= in_patient_id;
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: oral_hairy_leukoplakia): #{patient_id}"

                end #oral_hairy_leukoplakia

                if (answer == 'Papular pruritic eruptions / Fungal nail infections')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET papular_pruritic_eruptions = NULL WHERE flat_table1.patient_id= in_patient_id;
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: papular_pruritic_eruptions): #{patient_id}"

                end #papular_pruritic_eruptions

                if (answer == 'Parotid enlargement, persistent unexplained')
                  Connection.execute <<EOF
                  UPDATE flat_table1 SET parotid_enlargement_persistent_unexplained = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: parotid_enlargement_persistent_unexplained): #{patient_id}"

                end #parotid_enlargement_persistent_unexplained

                if (answer == 'Persistent generalized lymphadenopathy')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET persistent_generalized_lymphadenopathy = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: persistent_generalized_lymphadenopathy): #{patient_id}"

                end #persistent_generalized_lymphadenopathy

                if (answer == 'Pneumocystis pneumonia')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET pnuemocystis_pnuemonia = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: pnuemocystis_pnuemonia): #{patient_id}"

                end #pnuemocystis_pnuemonia

                if (answer == 'Progressive multifocal leukoencephalopathy')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET progressive_multifocal_leukoencephalopathy = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: progressive_multifocal_leukoencephalopathy): #{patient_id}"

                end #progressive_multifocal_leukoencephalopathy

                if (answer == 'Pulmonary tuberculosis (current)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET pulmonary_tuberculosis_last_2_years = NULL, pulmonary_tuberculosis_last_2_years_v_date = '#{Current_date}' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: pulmonary_tuberculosis_last_2_years): #{patient_id}"

                end #pulmonary_tuberculosis_last_2_years

                if (answer == 'Respiratory tract infections, recurrent (sinusitis, tonsilitus, otitis media, pharyngitis)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET respiratory_tract_infections_recurrent = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: respiratory_tract_infections_recurrent): #{patient_id}"

                end #respiratory_tract_infections_recurrent

                if (answer == 'Seborrhoeic dermatitis')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET seborrhoeic_dermatitis = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: seborrhoeic_dermatitis): #{patient_id}"

                end #seborrhoeic_dermatitis

                if (answer == 'Severe bacterial infections (pneumonia, empyema, pyomyositis, bone/joint, meningitis, bacteraemia)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET severe_bacterial_infection = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: severe_bacterial_infection): #{patient_id}"

                end #severe_bacterial_infection

                if (answer == 'Severe unexplained wasting or malnutrition not responding to treatment (weight-for-height/ -age <70% or MUAC less than 11cm or oedema)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET severe_weight_loss = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: severe_weight_loss): #{patient_id}"

                end #severe_weight_loss

                if (answer == 'Severe weight loss >10% and/or BMI <18.5kg/m^2, unexplained')
                  Connection.execute <<EOF
                                  UPDATE flat_table1 SET severe_weight_loss = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: severe_weight_loss): #{patient_id}"

                end #severe_weight_loss

                if (answer == 'Symptomatic HIV-associated nephropathy or cardiomyopathy')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET symptomatic_hiv_associated_nephropathy = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: symptomatic_hiv_associated_nephropathy): #{patient_id}"

                end #symptomatic_hiv_associated_nephropathy

                if (answer == 'Thrombocytopaenia, chronic < 50,000 /mm(cubed)')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET thrombocytopaenia_chronic = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: thrombocytopaenia_chronic): #{patient_id}"

                end #thrombocytopaenia_chronic

                if (answer == 'Toxoplasmosis of the brain')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET toxoplasmosis_of_the_brain  = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: toxoplasmosis_of_the_brain): #{patient_id}"

                end #toxoplasmosis_of_the_brain

                if (answer == 'Tuberculosis (PTB or EPTB) within the last 2 years')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET pulmonary_tuberculosis_last_2_years = NULL, pulmonary_tuberculosis_last_2_years_v_date = '#{Current_date}' WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: pulmonary_tuberculosis_last_2_years): #{patient_id}"

                end #pulmonary_tuberculosis_last_2_years

                if (answer == 'Unspecified stage I condition')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET unspecified_stage_1_cond = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: unspecified_stage_1_cond): #{patient_id}"

                end #unspecified_stage_1_cond

                if (answer == 'Unspecified stage II condition')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET unspecified_stage2_condition = NULL WHERE flat_table1.patient_id= in_patient_id;
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: unspecified_stage2_condition): #{patient_id}"

                end #unspecified_stage2_condition

                if (answer == 'Unspecified stage III condition')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET unspecified_stage3_condition = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: unspecified_stage3_condition): #{patient_id}"

                end #unspecified_stage3_condition

                if (answer == 'Unspecified stage IV condition')
                  Connection.execute <<EOF
                    UPDATE flat_table1 SET unspecified_stage_4_condition = NULL WHERE flat_table1.patient_id= "#{patient_id}";
EOF
                  puts ".......... Updating record into flat_table1 (who_crit_stage: unspecified_stage_4_condition): #{patient_id}"

                end #unspecified_stage_4_condition

              end #voided

            end #case concept_id

          end #voided

        end #case concept_id

        



        current_hiv_program_state_record = Connection.select_one("SELECT * FROM flat_table2 WHERE id = '#{visit}'")

        current_hiv_program_state = current_hiv_program_state_record['current_hiv_program_state']

        current_state_record = Connection.select_one("SELECT IFNULL(current_state_for_program(#{patient_id},1,'#{Current_date}'), 'Unknown') AS state")

        current_state = current_state_record['state']       

        if current_hiv_program_state.blank?
          patient_program_record = Connection.select_one("SELECT * FROM patient_program WHERE patient_id = #{patient_id}
            AND program_id = 1")

          patient_program_id = patient_program_record['patient_program_id']

          patient_state_record = Connection.select_one("SELECT * FROM patient_state WHERE patient_program_id = #{patient_program_id}
            AND start_date <= '#{Current_date}'
            ORDER BY patient_state_id DESC")

          latest_patient_hiv_state = patient_state_record['state']

          current_hiv_state_record = Connection.select_one("SELECT * FROM program_workflow_state pws
                                      LEFT OUTER JOIN concept_name c ON c.concept_id = pws.concept_id
                                      WHERE pws.program_workflow_id = 1
                                      AND pws.program_workflow_state_id = '#{current_state}'
                                      AND pws.retired = 0")
          current_hiv_state = current_hiv_state_record['name']

          if current_hiv_state.blank?
            Connection.execute <<EOF
            UPDATE flat_table2
            SET current_hiv_program_state = "#{current_hiv_state}", current_hiv_program_start_date = "#{Current_date}"
            WHERE flat_table2.id = "#{visit}";
EOF
          end
        end
      end
    end
    end
  end
end

def updating_drug_orders_table(patient_ids)
  (patient_ids || []).each do |patient_id|
    drug_order_rec = Connection.select_all("
      SELECT * FROM drug_order d 
      INNER JOIN orders o ON d.order_id = o.order_id
      INNER JOIN encounter e ON o.encounter_id = e.encounter_id
      WHERE e.patient_id = #{patient_id}
      AND (DATE(o.date_created) = '#{Current_date}' OR  
      DATE(o.date_voided) = '#{Current_date}')
      AND o.voided = 0 ")

    (drug_order_rec).each do |drug_order|

      order_id = drug_order['order_id']

      drug_inventory_id = drug_order['drug_inventory_id']

      equivalent_daily_dose = drug_order['equivalent_daily_dose']

      dose = drug_order['dose']

      frequency = drug_order['frequency']

      quantity = drug_order['quantity']

      flat_table2_record = Connection.select_one("SELECT * FROM flat_table2 
        WHERE drug_order_id1 = '#{order_id}' OR drug_order_id2 = '#{order_id}' OR drug_order_id3 = '#{order_id}' OR
        drug_order_id4 = '#{order_id}' OR drug_order_id5 = '#{order_id}'")

      visit = flat_table2_record['ID'] rescue nil


      drug_set = Connection.select_one("SELECT * FROM flat_table2 WHERE ID = '#{visit}' ")

      drug_name_rec = Connection.select_one("SELECT * FROM drug WHERE drug_id = '#{drug_inventory_id}'")

      orders_rec = Connection.select_one("SELECT * FROM orders WHERE order_id = '#{order_id}'")


      drug_set1 = drug_set['drug_order_id1'] rescue nil

      drug_set2 = drug_set['drug_order_id2'] rescue nil

      drug_set3 = drug_set['drug_order_id3'] rescue nil
    
      drug_set4 = drug_set['drug_order_id4'] rescue nil
    
      drug_set5 = drug_set['drug_order_id5'] rescue nil
      

      drug_name = drug_name_rec['name']

      encounter_id = orders_rec['encounter_id']

      #raise encounter_id.inspect

      unless drug_set1.blank?
        Connection.execute <<EOF
            UPDATE flat_table2 SET drug_inventory_id1 = "#{drug_inventory_id}", drug_name1 = @"#{drug_name}", 
                drug_equivalent_daily_dose1 = "#{equivalent_daily_dose}", drug_dose1 = "#{dose}",
                drug_frequency1 = "#{frequency}", drug_quantity1 = "#{quantity}", drug_order_id1 = "#{order_id}",
                drug_inventory_id1_enc_id = "#{encounter_id}", drug_name1_enc_id = "#{encounter_id}", 
                drug_equivalent_daily_dose1_enc_id = "#{encounter_id}", drug_dose1_enc_id = "#{encounter_id}",
                drug_frequency1_enc_id = "#{encounter_id}", drug_quantity1_enc_id = "#{encounter_id}", drug_order_id1_enc_id = "#{encounter_id}"
            WHERE flat_table2.id = "#{visit}";
EOF
        puts "........... Updating flat_table2 (Drug orders: drug_set1)"
      end

      unless drug_set2.blank?
        Connection.execute <<EOF
          UPDATE flat_table2 SET drug_inventory_id2 = "#{drug_inventory_id}", drug_name2 = "#{drug_name}", 
                drug_equivalent_daily_dose2 = "#{equivalent_daily_dose}", drug_dose2 = "#{dose}",
                drug_frequency2 = "#{frequency}", drug_quantity2 = "#{quantity}", drug_order_id2 = "#{order_id}",
                drug_inventory_id2_enc_id = "#{encounter_id}", drug_name2_enc_id = "#{encounter_id}", 
                drug_equivalent_daily_dose2_enc_id = "#{encounter_id}", drug_dose2_enc_id = "#{encounter_id}",
                drug_frequency2_enc_id = "#{encounter_id}", drug_quantity2_enc_id = "#{encounter_id}", drug_order_id2_enc_id = "#{encounter_id}" 
          WHERE flat_table2.id = "#{visit}";
EOF
      end

      unless drug_set3.blank?
        Connection.execute <<EOF
          UPDATE flat_table2 SET drug_inventory_id3 = "#{drug_inventory_id}", drug_name3 = "#{drug_name}", 
                drug_equivalent_daily_dose3 = "#{equivalent_daily_dose}", drug_dose3 = "#{dose}",
                drug_frequency3 = "#{frequency}", drug_quantity3 = "#{quantity}", drug_order_id3 = "#{order_id}",
                drug_inventory_id3_enc_id = "#{encounter_id}", drug_name3_enc_id = "#{encounter_id}", 
                drug_equivalent_daily_dose3_enc_id = "#{encounter_id}", drug_dose3_enc_id = "#{encounter_id}",
                drug_frequency3_enc_id = "#{encounter_id}", drug_quantity3_enc_id = "#{encounter_id}", drug_order_id3_enc_id =  "#{encounter_id}"   
          WHERE flat_table2.id = "#{visit}";
EOF
      end

      unless drug_set4.blank?
        Connection.execute <<EOF
          UPDATE flat_table2 SET drug_inventory_id4 = "#{drug_inventory_id}", drug_name4 = "#{drug_name}", 
                drug_equivalent_daily_dose4 = "#{equivalent_daily_dose}", drug_dose4 = "#{dose}",
                drug_frequency4 = "#{frequency}", drug_quantity4 = "#{quantity}", drug_order_id4 = "#{order_id}",
                drug_inventory_id4_enc_id = "#{encounter_id}", drug_name4_enc_id = "#{encounter_id}", 
                drug_equivalent_daily_dose4_enc_id = "#{encounter_id}", drug_dose4_enc_id = "#{encounter_id}",
                drug_frequency4_enc_id = "#{encounter_id}", drug_quantity4_enc_id = "#{encounter_id}", drug_order_id4_enc_id =  "#{encounter_id}"   
          WHERE flat_table2.id = "#{visit}";
EOF
      end

      unless drug_set5.blank?
        Connection.execute <<EOF
          UPDATE flat_table2 SET drug_inventory_id5 = "#{drug_inventory_id}", drug_name5 = "#{drug_name}", 
                drug_equivalent_daily_dose5 = "#{equivalent_daily_dose}", drug_dose5 = "#{dose}",
                drug_frequency5 = "#{frequency}", drug_quantity5 = "#{quantity}", drug_order_id5 = "#{order_id}",
                drug_inventory_id5_enc_id = "#{encounter_id}", drug_name5_enc_id = "#{encounter_id}", 
                drug_equivalent_daily_dose5_enc_id = "#{encounter_id}", drug_dose5_enc_id = "#{encounter_id}",
                drug_frequency5_enc_id = "#{encounter_id}", drug_quantity5_enc_id = "#{encounter_id}", drug_order_id5_enc_id = "#{encounter_id}"      
          WHERE flat_table2.id = "#{visit}";

EOF
      end

      visit = 0

    end

  end
  
end

def updating_orders_tables(patient_ids)
  (patient_ids || []).each do |patient_id|

    flat_table2_record = Connection.select_one("SELECT * FROM flat_table2 WHERE patient_id = #{patient_id} AND (DATE(visit_date) = '#{Current_date}')")
    
    next if flat_table2_record.blank?

    visit = flat_table2_record['ID']

    drug_set = Connection.select_one("SELECT * FROM flat_table2 WHERE ID = #{visit}")

    drug_set1 = drug_set['drug_order_id1']
    
    drug_set2 = drug_set['drug_order_id2']
    
    drug_set3 = drug_set['drug_order_id3']
    
    drug_set4 = drug_set['drug_order_id4']
    
    drug_set5 = drug_set['drug_order_id5']

    orders_rec = Connection.select_all("
      SELECT * FROM orders o
      INNER JOIN encounter e ON o.encounter_id = e.encounter_id
      WHERE e.patient_id = #{patient_id}
      AND (DATE(o.date_created) = '#{Current_date}' OR  
      DATE(o.date_voided) = '#{Current_date}')
      AND o.voided = 0 ")


    
    (orders_rec).each do |order|
      #raise order['order_id'].inspect

      order_id = order['order_id']
      encounter_id = order['encounter_id']
      start_date = order['start_date']
      auto_expire_date = order['auto_expire_date']
      voided = order['voided']

      if drug_set1.blank?
        if voided == 0
          if visit.blank?
            Connection.execute <<EOF
              INSERT INTO flat_table2 (patient_id, visit_date, drug_order_id1, 
                drug_encounter_id1, drug_start_date1, drug_auto_expire_date1, 
                drug_order_id1_enc_id, drug_encounter_id1_enc_id, drug_start_date1_enc_id, drug_auto_expire_date1_enc_id) 
              VALUES ("#{patient_id}", "#{Current_date}", "#{order_id}", "#{encounter_id}", 
                "#{start_date}", "#{auto_expire_date}", "#{encounter_id}", "#{encounter_id}", "#{encounter_id}", "#{encounter_id}");
EOF
      
          else
            Connection.execute <<EOF
              UPDATE flat_table2 SET drug_order_id1 = "#{order_id}", drug_encounter_id1 = "#{encounter_id}", drug_start_date1 = "#{start_date}", drug_auto_expire_date1 = "#{auto_expire_date}",
                    drug_order_id1_enc_id = "#{encounter_id}", drug_encounter_id1_enc_id = "#{encounter_id}", 
                    drug_start_date1_enc_id = "#{encounter_id}", drug_auto_expire_date1_enc_id = "#{encounter_id}" 
              WHERE flat_table2.id = "#{visit}";
EOF
          end
        else
          Connection.execute <<EOF
            UPDATE flat_table2 SET drug_order_id1 = NULL, drug_encounter_id1 = NULL, 
                      drug_start_date1 = NULL, drug_auto_expire_date1 = NULL,
                      drug_order_id1_enc_id = NULL, drug_encounter_id1_enc_id = NULL, 
                      drug_start_date1_enc_id = NULL, drug_auto_expire_date1_enc_id = NULL 
            WHERE flat_table2.id = "#{visit}";
EOF
        end
      end

      if drug_set2.blank?
        if voided == 0
          if visit.blank?
            Connection.execute <<EOF
              INSERT INTO flat_table2 (patient_id, visit_date, drug_order_id2, 
                    drug_encounter_id2, drug_start_date2, drug_auto_expire_date2, drug_order_id2_enc_id, 
                    drug_encounter_id2_enc_id, drug_start_date2_enc_id, drug_auto_expire_date2_enc_id) 
              VALUES ("#{patient_id}", "#{Current_date}", "#{order_id}", "#{encounter_id}", 
                    "#{start_date}", "#{auto_expire_date}", "#{encounter_id}", "#{encounter_id}", "#{encounter_id}", "#{encounter_id}");
EOF
          else
            Connection.execute <<EOF
              UPDATE flat_table2 SET drug_order_id2 = "#{order_id}", drug_encounter_id2 = "#{encounter_id}", 
                    drug_start_date2 = "#{start_date}", drug_auto_expire_date2 = "#{auto_expire_date}" ,
                    drug_order_id2_enc_id = "#{encounter_id}", drug_encounter_id2_enc_id = "#{encounter_id}", 
                    drug_start_date2_enc_id = "#{encounter_id}", drug_auto_expire_date2_enc_id = "#{encounter_id}"
              WHERE flat_table2.id = "#{visit}";
EOF
          end

        else
          Connection.execute <<EOF
            UPDATE flat_table2 SET drug_order_id2 = NULL, drug_encounter_id2 = NULL, 
                      drug_start_date2 = NULL, drug_auto_expire_date2 = NULL ,
                      drug_order_id2_enc_id = NULL, drug_encounter_id2_enc_id = NULL, 
                      drug_start_date2_enc_id = NULL, drug_auto_expire_date2_enc_id = NULL
            WHERE flat_table2.id = "#{visit}";
EOF
        end
                  
      end

      if drug_set3.blank?
        if voided == 0
          if visit.blank?
            Connection.execute <<EOF
              INSERT INTO flat_table2 (patient_id, visit_date, drug_order_id3, 
                    drug_encounter_id3, drug_start_date3, drug_auto_expire_date3, drug_order_id3_enc_id, 
                    drug_encounter_id3_enc_id, drug_start_date3_enc_id, drug_auto_expire_date3_enc_id) 
              VALUES ("#{patient_id}", "#{Current_date}", "#{order_id}", "#{encounter_id}", 
                    "#{start_date}", "#{auto_expire_date}", "#{encounter_id}", "#{encounter_id}", "#{encounter_id}", "#{encounter_id}");
EOF
          else 
            Connection.execute <<EOF
              UPDATE flat_table2 SET drug_order_id3 = "#{order_id}", drug_encounter_id3 = "#{encounter_id}", 
                    drug_start_date3 = "#{start_date}", drug_auto_expire_date3 = "#{auto_expire_date}",
                    drug_order_id3_enc_id = "#{encounter_id}", drug_encounter_id3_enc_id = "#{encounter_id}", 
                    drug_start_date3_enc_id = "#{encounter_id}", drug_auto_expire_date3_enc_id = "#{encounter_id}" 
              WHERE flat_table2.id = "#{visit}";
EOF
          end

        else
          Connection.execute <<EOF
            UPDATE flat_table2 SET drug_order_id3 = NULL, drug_encounter_id3 = NULL, 
                      drug_start_date3 = NULL, drug_auto_expire_date3 = NULL,
                      drug_order_id3_enc_id = NULL, drug_encounter_id3_enc_id = NULL, 
                      drug_start_date3_enc_id = NULL, drug_auto_expire_date3_enc_id = NULL 
            WHERE flat_table2.id = "#{visit}";
EOF
        end
        
      end

      if drug_set4.blank?
        if voided == 0
          if visit.blank?
            Connection.execute <<EOF
              INSERT INTO flat_table2 (patient_id, visit_date, drug_order_id4, 
                    drug_encounter_id4, drug_start_date4, drug_auto_expire_date4, drug_order_id4_enc_id, 
                    drug_encounter_id4_enc_id, drug_start_date4_enc_id, drug_auto_expire_date4_enc_id) 
              VALUES ("#{patient_id}", "#{Current_date}", "#{order_id}", "#{encounter_id}", 
                    "#{start_date}", "#{auto_expire_date}", "#{encounter_id}", "#{encounter_id}", "#{encounter_id}", "#{encounter_id}");
EOF

          else
            Connection.execute <<EOF
              UPDATE flat_table2 SET drug_order_id4 = "#{order_id}", drug_encounter_id4 = "#{encounter_id}", 
                    drug_start_date4 = "#{start_date}", drug_auto_expire_date4 = "#{auto_expire_date}",
                    drug_order_id4_enc_id = "#{encounter_id}", drug_encounter_id4_enc_id = "#{encounter_id}", 
                    drug_start_date4_enc_id = "#{encounter_id}", drug_auto_expire_date4_enc_id = "#{encounter_id}" 
              WHERE flat_table2.id = "#{visit}";
EOF
          end

        else
          Connection.execute <<EOF
            UPDATE flat_table2 SET drug_order_id4 = NULL, drug_encounter_id4 = NULL, 
                      drug_start_date4 = NULL, drug_auto_expire_date4 = NULL,
                      drug_order_id4_enc_id = NULL, drug_encounter_id4_enc_id = NULL, 
                      drug_start_date4_enc_id = NULL, drug_auto_expire_date4_enc_id = NULL
            WHERE flat_table2.id = "#{visit}";
EOF
        end
        
      end

      if drug_set5.blank?
        if voided == 0
          if visit.blank?
            Connection.execute <<EOF
              INSERT INTO flat_table2 (patient_id, visit_date, drug_order_id5, 
                    drug_encounter_id5, drug_start_date5, drug_auto_expire_date5, drug_order_id5_enc_id, 
                    drug_encounter_id5_enc_id, drug_start_date5_enc_id, drug_auto_expire_date5_enc_id) 
              VALUES ("#{patient_id}", in_visit_date, "#{order_id}", "#{encounter_id}", 
                    "#{start_date}", "#{auto_expire_date}", "#{encounter_id}", "#{encounter_id}", "#{encounter_id}", "#{encounter_id}");
EOF
          else
            Connection.execute <<EOF
              UPDATE flat_table2 SET drug_order_id5 = "#{order_id}", drug_encounter_id5 = "#{encounter_id}", 
                    drug_start_date5 = "#{start_date}", drug_auto_expire_date5 = "#{auto_expire_date}",
                    drug_order_id5_enc_id = "#{encounter_id}", drug_encounter_id5_enc_id = "#{encounter_id}", 
                    drug_start_date5_enc_id = "#{encounter_id}", drug_auto_expire_date5_enc_id = "#{encounter_id}" 
              WHERE flat_table2.id = "#{visit}";
EOF
          end
          
        else
          Connection.execute <<EOF
            UPDATE flat_table2 SET drug_order_id5 = NULL, drug_encounter_id5 = NULL, 
                      drug_start_date5 = NULL, drug_auto_expire_date5 = NULL,
                      drug_order_id5_enc_id = NULL, drug_encounter_id5_enc_id = NULL, 
                      drug_start_date5_enc_id = NULL, drug_auto_expire_date5_enc_id = NULL 
            WHERE flat_table2.id = "#{visit}";
EOF
        end
        
      end

    end

  end
end

def upadating_other_fields(patient_ids)
  
end

def upating_relationship_table(patient_ids)

  (patient_ids || []).each do |person_id|
    relationship_rec = Connection.select_all("
      SELECT * FROM relationship
      WHERE person_a = '#{person_id}' AND
      (DATE(date_created) = '#{Current_date}' OR 
      DATE(date_voided) = '#{Current_date}')
    ")

    
    (relationship_rec).each do |relationship|
      #--Get the guardian's person_id
      guardian_id = relationship['person_b']

      #--Check if guardian exist in flat_table1
      flat_table1_record = Connection.select_one("SELECT * FROM flat_table1 WHERE patient_id = #{guardian_id}")

      guardian_id_in_flat_table = flat_table1_record['patient_id']

      #--Get patient_id
      guardian_to_which_patient = relationship['person_a']

      unless guardian_to_which_patient.blank?
        #--check if the giardian_person_id fields have values
        flat_table1_record2 = Connection.select_one("SELECT * FROM flat_table1 WHERE patient_id = #{guardian_to_which_patient}")

        guardian_person_id1 = flat_table1_record2['guardian_person_id1'] rescue nil
        guardian_person_id2 = flat_table1_record2['guardian_person_id2'] rescue nil
        guardian_person_id3 = flat_table1_record2['guardian_person_id3'] rescue nil
        guardian_person_id4 = flat_table1_record2['guardian_person_id4'] rescue nil
        guardian_person_id5 = flat_table1_record2['guardian_person_id5'] rescue nil

        if (guardian_person_id1 == guardian_id)
          guardian_exist = guardian_id

        elsif (guardian_person_id2 == guardian_id) 
          guardian_exist = guardian_id

        elsif (guardian_person_id3 == guardian_id)
          guardian_exist = guardian_id

        elsif (guardian_person_id4 == guardian_id)
          guardian_exist = guardian_id

        elsif (guardian_person_id5 == guardian_id)
          guardian_exist = guardian_id
        else
          guardian_exist = 0
        end

      end

      unless guardian_id.blank?
        #--check if the guardian_to_which_patient fields have values
        guardian_to_which_patient1 = flat_table1_record['guardian_to_which_patient1'] rescue nil
        guardian_to_which_patient2 = flat_table1_record['guardian_to_which_patient2'] rescue nil
        guardian_to_which_patient3 = flat_table1_record['guardian_to_which_patient3'] rescue nil
        guardian_to_which_patient4 = flat_table1_record['guardian_to_which_patient4'] rescue nil
        guardian_to_which_patient5 = flat_table1_record['guardian_to_which_patient5'] rescue nil

        if (guardian_to_which_patient1 == guardian_to_which_patient)
          patient_exist = guardian_to_which_patient

        elsif (guardian_to_which_patient2 == guardian_to_which_patient)
          patient_exist = guardian_to_which_patient

        elsif (guardian_to_which_patient3 == guardian_to_which_patient)
          patient_exist = guardian_to_which_patient

        elsif (guardian_to_which_patient4 == guardian_to_which_patient)
          patient_exist = guardian_to_which_patient

        elsif (guardian_to_which_patient5 == guardian_to_which_patient)
          patient_exist = guardian_to_which_patient
          
        else
          patient_exist = 0
        end

      end

      #--update the guardian_person_id fields

      if guardian_exist.blank?
        if guardian_person_id1.blank?
          Connection.execute <<EOF
          UPDATE flat_table1
          SET guardian_to_which_patient1 = "#{guardian_to_which_patient}"
          WHERE patient_id = "#{guardian_id}";
EOF
        elsif guardian_person_id2.blank?
          Connection.execute <<EOF
          UPDATE flat_table1
          SET guardian_person_id2 = "#{guardian_id}"
          WHERE patient_id = "#{guardian_to_which_patient}";
EOF
        elsif guardian_person_id3.blank?
          Connection.execute <<EOF
          UPDATE flat_table1
          SET guardian_person_id3 = "#{guardian_id}"
          WHERE patient_id = "#{guardian_to_which_patient}";
EOF
        elsif guardian_person_id4.blank?
          Connection.execute <<EOF
          UPDATE flat_table1
          SET guardian_person_id4 = "#{guardian_id}"
          WHERE patient_id = "#{guardian_to_which_patient}";
EOF
        elsif guardian_person_id5.blank?
          Connection.execute <<EOF
          UPDATE flat_table1
          SET guardian_person_id5 = "#{guardian_id}"
          WHERE patient_id = "#{guardian_to_which_patient}";
EOF
        else
          guardian_person = guardian_id
        end
      end

      #--update the guardian_to_which_patient fields
      if patient_exist.blank?
        if guardian_to_which_patient1.blank?
          Connection.execute <<EOF
          UPDATE flat_table1
          SET guardian_to_which_patient1 = "#{guardian_to_which_patient}"
          WHERE patient_id = "#{guardian_id}";
EOF
        elsif guardian_to_which_patient2.blank?
          Connection.execute <<EOF
          UPDATE flat_table1
          SET guardian_to_which_patient2 = @guardian_to_which_patient
          WHERE patient_id = @guardian_id;
EOF
        elsif guardian_to_which_patient3.blank?
          Connection.execute <<EOF
          UPDATE flat_table1
          SET guardian_to_which_patient3 = "#{guardian_to_which_patient}"
          WHERE patient_id = "#{guardian_id}";
EOF
        elsif guardian_to_which_patient4.blank?
          Connection.execute <<EOF
          UPDATE flat_table1
          SET guardian_to_which_patient4 = "#{guardian_to_which_patient}"
          WHERE patient_id = "#{guardian_id}";
EOF
        elsif guardian_to_which_patient5.blank?
          Connection.execute <<EOF
          UPDATE flat_table1
          SET guardian_to_which_patient5 = "#{guardian_to_which_patient}"
          WHERE patient_id = "#{guardian_id}";
EOF
        else
          guardian_to_which_patient_id = guardian_to_which_patient
        end
      end
  end

  end
end

def updating_patient_program_table(patient_ids)
  
  data = get_earliest_start_date_patients_data(patient_ids)
  (data || []).each do |row|
  

      #raise row.inspect

      flat_table1_record = Connection.select_one("SELECT * FROM flat_table1 WHERE patient_id = #{row[:patient_id]}")  

      earliest_start_date = flat_table1_record['earliest_start_date'].to_date rescue nil
      date_enrolled       = flat_table1_record['date_enrolled'].to_date rescue nil
      age_at_initiation   = flat_table1_record['age_at_initiation'] 
      age_in_days         = flat_table1_record['age_in_days']

      unless row[:earliest_start_date] == earliest_start_date
        earliest_start_date = row[2]
      end unless row[:earliest_start_date].blank? rescue nil

      unless row[:date_enrolled] == date_enrolled
        date_enrolled = row[1]
      end unless row[:date_enrolled].blank? rescue nil
      
      unless row[:age_at_initiation] == age_at_initiation
        age_at_initiation = row[:age_at_initiation]
      end
        
      unless row[:age_in_days] == age_in_days
        age_in_days = row[:age_in_days]
      end
     
      if not earliest_start_date.blank? and not date_enrolled.blank?
        Connection.execute <<EOF
UPDATE flat_table1 
  SET earliest_start_date = '#{earliest_start_date}', date_enrolled = '#{date_enrolled}', 
  age_at_initiation = #{age_at_initiation}, age_in_days = #{age_in_days} WHERE patient_id = #{row[:patient_id]};
EOF
  
      elsif earliest_start_date.blank? and date_enrolled.blank?
        Connection.execute <<EOF
UPDATE flat_table1 
  SET age_at_initiation = #{age_at_initiation}, age_in_days = #{age_in_days} WHERE patient_id = #{row[:patient_id]};
EOF
  
      end
  puts "........... Updating record into flat_table1 (Patient_program): #{row[0]}"
      
  end
end

def updating_person_attributes_table(patient_ids)
  occupation_id = PersonAttributeType.find_by_name('Occupation').id
  cell_phone_number_id = PersonAttributeType.find_by_name('Cell Phone Number').id
  home_phone_number_id = PersonAttributeType.find_by_name('Home Phone Number').id
  office_phone_number_id = PersonAttributeType.find_by_name('Office Phone Number').id

  #puts "occupation id: #{occupation_id}"

  (patient_ids || []).each do |person_id|
    person_attributes_records = Connection.select_all("
      SELECT * FROM person_attribute WHERE person_id = #{person_id}
      AND (DATE(date_created) = '#{Current_date}' 
      OR DATE(date_voided) = '#{Current_date}') AND voided = 0")

    flat_table1_record = Connection.select_one("SELECT * FROM flat_table1 WHERE patient_id = #{person_id}")  

    occupation = flat_table1_record['occupation']
    cell_phone_number = flat_table1_record['cellphone_number']
    home_phone_number = flat_table1_record['home_phone_number']
    office_phone_number = flat_table1_record['office_phone_number']

    #puts "occupation: #{occupation}"

    #1. updating occupation (New)
    occupation_rec = Connection.select_one("
      SELECT * FROM person_attribute WHERE person_id = #{person_id}
      AND person_attribute_type_id = #{occupation_id} 
      AND (DATE(date_created) = '#{Current_date}' 
      OR DATE(date_voided) = '#{Current_date}')
      AND voided = 0 ORDER BY date_created DESC LIMIT 1")

    unless occupation_rec['value'] == occupation
      occupation = occupation_rec['value']
      Connection.execute <<EOF
UPDATE flat_table1 
SET occupation = "#{occupation}" WHERE patient_id = #{person_id};
EOF

    end unless occupation_rec.blank?
  
    occupation_rec = Connection.select_one("
      SELECT * FROM person_attribute WHERE person_id = #{person_id}
      AND person_attribute_type_id = #{occupation_id} 
      AND (DATE(date_created) = '#{Current_date}' 
      OR DATE(date_voided) = '#{Current_date}')
      AND voided = 1 ORDER BY date_created DESC LIMIT 1")

    (occupation_rec || []).each do |occup|
      if occup['value'] == occupation
        Connection.execute <<EOF
UPDATE flat_table1 
SET occupation = NULL WHERE patient_id = #{person_id};
EOF
      end
    end
   #............................................................................................. occupation end
    
    #2. Updating cell phone number
     cell_phone_number_rec = Connection.select_one("
      SELECT * FROM person_attribute WHERE person_id = #{person_id}
      AND person_attribute_type_id = #{cell_phone_number_id} 
      AND (DATE(date_created) = '#{Current_date}' 
      OR DATE(date_voided) = '#{Current_date}')
      AND voided = 0 ORDER BY date_created DESC LIMIT 1")

     #puts "cell_phone_number_rec: #{cell_phone_number_rec}"

    unless cell_phone_number_rec['value'] == cell_phone_number
      cell_phone_number = cell_phone_number_rec['value']
      Connection.execute <<EOF
UPDATE flat_table1 
SET cellphone_number = "#{cell_phone_number}" WHERE patient_id = #{person_id};
EOF

    end unless cell_phone_number_rec.blank?
  
    cell_phone_number_rec = Connection.select_one("
      SELECT * FROM person_attribute WHERE person_id = #{person_id}
      AND person_attribute_type_id = #{cell_phone_number_id} 
      AND (DATE(date_created) = '#{Current_date}' 
      OR DATE(date_voided) = '#{Current_date}')
      AND voided = 1 ORDER BY date_created DESC LIMIT 1")



    (cell_phone_number_rec || []).each do |cellphone|
      if cellphone['value'] == cell_phone_number

      puts "updating person: #{person_id}"
        Connection.execute <<EOF
UPDATE flat_table1 
SET cellphone_number = NULL WHERE patient_id = #{person_id};
EOF
      end
    end

    #............................................................................ Cell_phone number end

    #3. Updating home phone number
     home_phone_number_rec = Connection.select_one("
      SELECT * FROM person_attribute WHERE person_id = #{person_id}
      AND person_attribute_type_id = #{home_phone_number_id} 
      AND (DATE(date_created) = '#{Current_date}' 
      OR DATE(date_voided) = '#{Current_date}')
      AND voided = 0 ORDER BY date_created DESC LIMIT 1")

     #puts "home_phone_number: #{home_phone_number_rec}"

    unless home_phone_number_rec['value'] == home_phone_number
      home_phone_number = home_phone_number_rec['value']
      Connection.execute <<EOF
UPDATE flat_table1 
SET home_phone_number = "#{home_phone_number}" WHERE patient_id = #{person_id};
EOF


    end unless home_phone_number_rec.blank?
  
    home_phone_number_rec = Connection.select_one("
      SELECT * FROM person_attribute WHERE person_id = #{person_id}
      AND person_attribute_type_id = #{home_phone_number_id} 
      AND (DATE(date_created) = '#{Current_date}' 
      OR DATE(date_voided) = '#{Current_date}')
      AND voided = 1 ORDER BY date_created DESC LIMIT 1")

    (cell_phone_number_rec || []).each do |homephone|
      if homephone['value'] == home_phone_number
        Connection.execute <<EOF
UPDATE flat_table1 
SET home_phone_number = NULL WHERE patient_id = #{person_id};
EOF
      end
    end

    #............................................................................. home_phone_number

    #4. Updating office_phone_number
     office_phone_number_rec = Connection.select_one("
      SELECT * FROM person_attribute WHERE person_id = #{person_id}
      AND person_attribute_type_id = #{office_phone_number_id} 
      AND (DATE(date_created) = '#{Current_date}' 
      OR DATE(date_voided) = '#{Current_date}')
      AND voided = 0 ORDER BY date_created DESC LIMIT 1")

    unless office_phone_number_rec['value'] == office_phone_number
      office_phone_number = office_phone_number_rec['value']
      Connection.execute <<EOF
UPDATE flat_table1
SET office_phone_number = "#{office_phone_number}" WHERE patient_id = #{person_id};
EOF

    end unless office_phone_number_rec.blank?
  
    office_phone_number_rec = Connection.select_one("
      SELECT * FROM person_attribute WHERE person_id = #{person_id}
      AND person_attribute_type_id = #{office_phone_number_id} 
      AND (DATE(date_created) = '#{Current_date}' 
      OR DATE(date_voided) = '#{Current_date}')
      AND voided = 1 ORDER BY date_created DESC LIMIT 1")

    (office_phone_number_rec || []).each do |officephone|
      if officephone['value'] == office_phone_number
        Connection.execute <<EOF
UPDATE flat_table1 
SET office_phone_number = NULL WHERE patient_id = #{person_id};
EOF
      end
    end

    #............................................................................ office number end

  end

end

def updating_patient_identifier_table(patient_ids)
  nat_id_type_id = PatientIdentifierType.find_by_name('National id').id
  arv_number_type_id = PatientIdentifierType.find_by_name('ARV number').id
  pre_arv_number_type_id = PatientIdentifierType.find_by_name('Pre-art number').id
  filing_number_id = PatientIdentifierType.find_by_name('Filing number').id
  archived_filing_number_id = PatientIdentifierType.find_by_name('Archived filing number').id

  #puts "archived_filing_number_id: #{archived_filing_number_id}"


  (patient_ids || []).each do |patient_identifier_patient_id|
    patient_identifier_records = Connection.select_all("
      SELECT * FROM patient_identifier WHERE patient_id = #{patient_identifier_patient_id}
      AND (DATE(date_created) = '#{Current_date}' 
      OR DATE(date_voided) = '#{Current_date}') AND voided = 0")

    patient_id = patient_identifier_patient_id.to_i 
    flat_table1_record = Connection.select_one("SELECT * FROM flat_table1 WHERE patient_id = #{patient_id}")  
      
    (patient_identifier_records || []).each do |row|

      old_nat_id = flat_table1_record['nat_id']
      arv_number = flat_table1_record['arv_number']
      pre_art_number = flat_table1_record['pre_art_number']
      new_nat_id = flat_table1_record['new_nat_id']
      filing_number = flat_table1_record['filing_number']
      archived_filing_number = flat_table1_record['archived_filing_number']

      # updating filing number -------------------------------------------------

      openmrs_filing_number = Connection.select_one("
        SELECT * FROM patient_identifier WHERE patient_id = #{patient_id}
        AND identifier_type = #{filing_number_id}
        AND (DATE(date_created) = '#{Current_date}' 
        OR DATE(date_voided) = '#{Current_date}')
        AND voided = 0 ORDER BY date_created DESC LIMIT 1")

      unless openmrs_filing_number['identifier'] == filing_number
        filing_number = openmrs_filing_number['identifier']
        Connection.execute <<EOF
UPDATE flat_table1 
  SET filing_number = "#{filing_number}" WHERE patient_id = #{patient_id};
EOF

      end unless openmrs_filing_number.blank?
    
      openmrs_filing_number = Connection.select_all("
        SELECT * FROM patient_identifier WHERE patient_id = #{patient_id}
        AND identifier_type = #{filing_number_id} AND LENGTH(identifier) = 6 
        AND (DATE(date_created) = '#{Current_date}' 
        OR DATE(date_voided) = '#{Current_date}')
        AND voided = 1")

      (openmrs_filing_number || []).each do |filing_num|
        if filing_num['identifier'] ==  filing_number
          Connection.execute <<EOF
UPDATE flat_table1 
  SET filing_number = NULL WHERE patient_id = #{patient_id};
EOF
        end
      end
     #....................................................................................... Filing number end


     # Updating archived_filing_number ............................................................... begin

     openmrs_archived_filing_number = Connection.select_one("
        SELECT * FROM patient_identifier WHERE patient_id = #{patient_id}
        AND identifier_type = #{archived_filing_number_id}
        AND (DATE(date_created) = '#{Current_date}' 
        OR DATE(date_voided) = '#{Current_date}')
        AND voided = 0 ORDER BY date_created DESC LIMIT 1")

      unless openmrs_archived_filing_number['identifier'] == archived_filing_number
        archived_filing_number = openmrs_archived_filing_number['identifier']
        Connection.execute <<EOF
UPDATE flat_table1 
  SET archived_filing_number = "#{archived_filing_number}" WHERE patient_id = #{patient_id};
EOF

      end unless openmrs_archived_filing_number.blank?
    
      openmrs_archived_filing_number = Connection.select_all("
        SELECT * FROM patient_identifier WHERE patient_id = #{patient_id}
        AND identifier_type = #{archived_filing_number_id}
        AND (DATE(date_created) = '#{Current_date}' 
        OR DATE(date_voided) = '#{Current_date}')
        AND voided = 1")

      (openmrs_archived_filing_number || []).each do |arc_filing_num|
        if arc_filing_num['identifier'] ==  archived_filing_number
          Connection.execute <<EOF
UPDATE flat_table1 
  SET archived_filing_number = NULL WHERE patient_id = #{patient_id};
EOF
        end
      end
     # archived_filing_number...........................................................................................end



      #1. updating national Id (New: with 6 char)
      openmrs_new_nat_id = Connection.select_one("
        SELECT * FROM patient_identifier WHERE patient_id = #{patient_id}
        AND identifier_type = #{nat_id_type_id} AND LENGTH(identifier) = 6 
        AND (DATE(date_created) = '#{Current_date}' 
        OR DATE(date_voided) = '#{Current_date}')
        AND voided = 0 ORDER BY date_created DESC LIMIT 1")

      unless openmrs_new_nat_id['identifier'] == new_nat_id
        new_nat_id = openmrs_new_nat_id['identifier']
        Connection.execute <<EOF
UPDATE flat_table1 
  SET new_nat_id = "#{new_nat_id}" WHERE patient_id = #{patient_id};
EOF

      end unless openmrs_new_nat_id.blank?
    
      openmrs_new_nat_id = Connection.select_all("
        SELECT * FROM patient_identifier WHERE patient_id = #{patient_id}
        AND identifier_type = #{nat_id_type_id} AND LENGTH(identifier) = 6 
        AND (DATE(date_created) = '#{Current_date}' 
        OR DATE(date_voided) = '#{Current_date}')
        AND voided = 1")

      (openmrs_new_nat_id || []).each do |nat_num|
        if nat_num['identifier'] ==  new_nat_id
          Connection.execute <<EOF
UPDATE flat_table1 
  SET new_nat_id = NULL WHERE patient_id = #{patient_id};
EOF
        end
      end
     #............................................................................................. New national ids end
      
      
      
      
      #2. updating national Id (old: with 13 char)
      openmrs_old_nat_id = Connection.select_one("
        SELECT * FROM patient_identifier WHERE patient_id = #{patient_id}
        AND identifier_type = #{nat_id_type_id} AND LENGTH(identifier) != 6 
        AND (DATE(date_created) = '#{Current_date}' 
        OR DATE(date_voided) = '#{Current_date}')
        AND voided = 0 ORDER BY date_created DESC LIMIT 1")

      unless openmrs_old_nat_id['identifier'] == old_nat_id
        old_nat_id = openmrs_old_nat_id['identifier']
        Connection.execute <<EOF
UPDATE flat_table1 
  SET nat_id = "#{old_nat_id}" WHERE patient_id = #{patient_id};
EOF

      end unless openmrs_old_nat_id.blank?
      
      openmrs_old_nat_ids = Connection.select_all("
        SELECT * FROM patient_identifier WHERE patient_id = #{patient_id}
        AND identifier_type = #{nat_id_type_id} AND LENGTH(identifier) != 6 
        AND (DATE(date_created) = '#{Current_date}' 
        OR DATE(date_voided) = '#{Current_date}')
        AND voided = 1")

      (openmrs_old_nat_ids || []).each do |nat_num|
        if nat_num['identifier'] ==  new_nat_id
          Connection.execute <<EOF
UPDATE flat_table1 
  SET nat_id = NULL WHERE patient_id = #{patient_id};
EOF
        end
      end
     #............................................................................................. Old national ids end




      #3. updating ARV number (adding ARV number)
      openmrs_arv_number = Connection.select_one("
        SELECT * FROM patient_identifier WHERE patient_id = #{patient_id}
        AND identifier_type = #{arv_number_type_id} 
        AND (DATE(date_created) = '#{Current_date}' 
        OR DATE(date_voided) = '#{Current_date}')
        AND voided = 0 ORDER BY date_created DESC LIMIT 1")
      
      unless openmrs_arv_number['identifier'] == arv_number
        arv_number = openmrs_arv_number['identifier']
        Connection.execute <<EOF
UPDATE flat_table1 
  SET arv_number = "#{arv_number}" WHERE patient_id = #{patient_id};
EOF

      end unless openmrs_arv_number.blank?

      #updating ARV number (removing ARV number if voided)
      openmrs_arv_numbers = Connection.select_all("
        SELECT * FROM patient_identifier WHERE patient_id = #{patient_id}
        AND identifier_type = #{arv_number_type_id} 
        AND (DATE(date_created) = '#{Current_date}' 
        OR DATE(date_voided) = '#{Current_date}')
        AND voided = 1")
      
      (openmrs_arv_numbers || []).each do |arv_num|
        if arv_num['identifier'] ==  arv_number
          Connection.execute <<EOF
UPDATE flat_table1 
  SET arv_number = NULL WHERE patient_id = #{patient_id};
EOF
        end
      end
     #............................................................................................. ARV numbers end 


    
      puts "........... Updating record into flat_table1 (patient_identifier): #{patient_id}"
    end
  end

end

def updating_person_address_table(person_ids)
  (person_ids || []).each do |person_address_person_id|
    person_address_records = Connection.select_all("
      SELECT * FROM person_address WHERE person_id = #{person_address_person_id}
      AND (DATE(date_created) = '#{Current_date.to_date.strftime('%Y-%m-%d 00:00:00')}' 
      OR DATE(date_voided) = '#{Current_date.to_date.strftime('%Y-%m-%d 00:00:00')}')
      AND voided = 0")

    #puts "person address records: #{person_address_person_id}"

    (person_address_records || []).each do |row|
      person_id = row['person_id'].to_i 
      flat_table1_record = Connection.select_one("SELECT * FROM flat_table1 WHERE patient_id = #{person_id}")  
      current_address = flat_table1_record['current_address']
      home_district = flat_table1_record['home_district']
      ta = flat_table1_record['ta']
      landmark = flat_table1_record['landmark']

      unless row['city_village'] == current_address
        current_address = row['city_village']
      end

      unless row['address2'] == home_district
        home_district = row['address2']
      end

      unless row['county_district'] == ta
        ta = row['county_district']
      end

      unless row['address1'] == landmark
        landmark = row['address1']
      end

      Connection.execute <<EOF
UPDATE flat_table1 
  SET current_address = "#{current_address}", home_district = "#{home_district}", 
  ta = "#{ta}", landmark = "#{landmark}" WHERE patient_id = #{person_id};
EOF

      puts "........... Updating record into flat_table1 (address): #{person_id}"
    end
  end
end

def updating_person_name_table(person_ids)
  (person_ids || []).each do |person_name_person_id|
    person_name_records = Connection.select_all("
      SELECT * FROM person_name WHERE person_id = #{person_name_person_id}
      AND (DATE(date_created) = '#{Current_date.to_date.strftime('%Y-%m-%d 00:00:00')}' 
      OR DATE(date_changed) = '#{Current_date.to_date.strftime('%Y-%m-%d 00:00:00')}'
      OR DATE(date_voided) = '#{Current_date.to_date.strftime('%Y-%m-%d 00:00:00')}')
      AND voided = 0")
    
    (person_name_records || []).each do |row|
      person_id = row['person_id'].to_i 
      flat_table1_rocord = Connection.select_one("SELECT * FROM flat_table1 WHERE patient_id = #{person_id}")  
      given_name = flat_table1_rocord['given_name']
      middle_name = flat_table1_rocord['middle_name']
      family_name = flat_table1_rocord['family_name']

      unless row['given_name'] == given_name
        given_name = row['given_name']
      end

      unless row['middle_name'] == middle_name
        middle_name = row['middle_name']
      end

      unless row['family_name'] == family_name
        family_name = row['family_name']
      end

      puts "........... Updating record into flat_table1 (names): #{person_id}"
      Connection.execute <<EOF
UPDATE flat_table1 
  SET given_name = "#{given_name}", family_name = "#{family_name}" WHERE patient_id = #{person_id};
EOF
     
      unless middle_name.blank? 
        Connection.execute <<EOF
UPDATE flat_table1 
  SET middle_name = "#{middle_name}" WHERE patient_id = #{person_id};
EOF
     
     end
      
    end

  end
      
end

def updating_person_table(person_ids)
  (person_ids || []).each do |person_id|
    person_records = Connection.select_all("
      SELECT * FROM person WHERE person_id = #{person_id}
      AND (DATE(date_created) = '#{Current_date.to_date.strftime('%Y-%m-%d 00:00:00')}' 
      OR DATE(date_changed) = '#{Current_date.to_date.strftime('%Y-%m-%d 00:00:00')}'
      OR DATE(date_voided) = '#{Current_date.to_date.strftime('%Y-%m-%d 00:00:00')}')
      AND '#{Current_date.to_date.strftime('%Y-%m-%d 23:59:59')}'
    ")
    
    unless person_records.blank?
      (person_records || []).each do |row|
        flat_table1_rocord = Connection.select_one("SELECT * FROM flat_table1 WHERE patient_id = #{person_id}")  
        if flat_table1_rocord.blank?
          puts "........... Inserting new record into flat_table1: #{person_id}"
          Connection.execute <<EOF
INSERT INTO flat_table1 
(patient_id, gender, dob, dob_estimated) 
VALUES(#{person_id}, '#{row['gender']}', '#{row['birthdate']}', #{row['birthdate_estimated']})
EOF
  
        else
          person_table_dob = row['birthdate'].to_date rescue nil
          person_table_dob_estimated = row['birthdate_estimated'].to_i
          person_table_gender = row['gender'] 
          person_table_death_date = row['death_date'].to_date rescue nil
          person_voided = row['voided'].to_i 

          dob = flat_table1_rocord['dob'].to_date rescue nil
          dob_estimated = flat_table1_rocord['dob_estimated'].to_i 
          gender = flat_table1_rocord['gender']
          death_date = flat_table1_rocord['death_date'].to_date rescue nil

          unless dob == person_table_dob
            dob = person_table_dob 
          end

          unless person_table_dob == dob_estimated
            dob_estimated = person_table_dob_estimated
          end

          unless gender == person_table_gender
            gender = person_table_gender
          end

          unless person_table_death_date == death_date
            death_date = person_table_death_date
          end

          if person_voided == 1
            puts "........... Deleting record into flat_table1: #{person_id}"
            Connection.execute <<EOF
DELETE flat_table1 WHERE patient_id = #{person_id}
EOF
  
         else
           puts "........... Updating record into flat_table1: #{person_id}"
           Connection.execute <<EOF
UPDATE flat_table1 
  SET dob = '#{dob}', dob_estimated = #{dob_estimated}, gender = '#{gender}'
  WHERE patient_id = #{person_id};
EOF
  
           unless death_date.blank?
             Connection.execute <<EOF
UPDATE flat_table1 
  SET death_date = '#{death_date}' WHERE patient_id = #{person_id};
EOF
  
           end 
         end

        end
      end
    end

  end
end

def updating_encounter_table(patient_ids)
  art_encounters =  [
    'SOURCE OF REFERRAL','UPDATE HIV STATUS',
    'HIV CLINIC REGISTRATION','VITALS','HIV STAGING',
    'HIV CLINIC CONSULTATION','ART ADHERENCE','TREATMENT','DISPENSING'
  ]
  art_encounters_ids = EncounterType.find(:all, :conditions =>["name IN(?)",art_encounters]).map(&:id)

  (patient_ids || []).each do |patient_id|
    puts "encounter table: patient_id: #{patient_id}"
    encounters = Connection.select_all("
      SELECT * FROM encounter WHERE patient_id = #{patient_id}
      AND encounter_datetime BETWEEN '#{Current_date.to_date.strftime('%Y-%m-%d 00:00:00')}' 
      AND '#{Current_date.to_date.strftime('%Y-%m-%d 23:59:59')}' 
      AND encounter_type IN(#{art_encounters_ids.join(',')}) 
    ")

    if encounters.length > 0
      (encounters || []).each do |e|
        
      end
    end
  end
end


def get_earliest_start_date_patients
  art_patient_ids = Connection.select_all("
   select 
        `p`.`patient_id` AS `patient_id`,
        cast(patient_start_date(`p`.`patient_id`) as date) AS `date_enrolled`,
        date_antiretrovirals_started(`p`.`patient_id`, min(`s`.`start_date`)) AS `earliest_start_date`,
        `person`.`death_date` AS `death_date`,
        ((to_days(date_antiretrovirals_started(`p`.`patient_id`, min(`s`.`start_date`))) - to_days(`person`.`birthdate`)) / 365.25) AS `age_at_initiation`,
        (to_days(min(`s`.`start_date`)) - to_days(`person`.`birthdate`)) AS `age_in_days`
    from
        ((`patient_program` `p`
        left join `patient_state` `s` ON ((`p`.`patient_program_id` = `s`.`patient_program_id`)))
        left join `person` ON ((`person`.`person_id` = `p`.`patient_id`)))
    where
        (
          (`p`.`voided` = 0)
          and (`s`.`voided` = 0)
          and (`p`.`program_id` = 1)
          and (`s`.`state` = 7)
          and (DATE(`p`.`date_created`) = '#{Current_date}'
          OR DATE(`p`.`date_changed`) = '#{Current_date}')
        )
    group by `p`.`patient_id`
  ").collect{|p|p["patient_id"].to_i}

  return art_patient_ids
end

def get_earliest_start_date_patients_data(patient_ids)
  records = Connection.select_all("
   select 
        `p`.`patient_id` AS `patient_id`,
        cast(patient_start_date(`p`.`patient_id`) as date) AS `date_enrolled`,
        date_antiretrovirals_started(`p`.`patient_id`, min(`s`.`start_date`)) AS `earliest_start_date`,
        `person`.`death_date` AS `death_date`,
        ((to_days(date_antiretrovirals_started(`p`.`patient_id`, min(`s`.`start_date`))) - to_days(`person`.`birthdate`)) / 365.25) AS `age_at_initiation`,
        (to_days(min(`s`.`start_date`)) - to_days(`person`.`birthdate`)) AS `age_in_days`
    from
        ((`patient_program` `p`
        left join `patient_state` `s` ON ((`p`.`patient_program_id` = `s`.`patient_program_id`)))
        left join `person` ON ((`person`.`person_id` = `p`.`patient_id`)))
    where
        (
          (`p`.`voided` = 0)
          and (`s`.`voided` = 0)
          and (`p`.`program_id` = 1)
          and (`s`.`state` = 7)
          and (DATE(`p`.`date_created`) = '#{Current_date}'
          OR DATE(`p`.`date_changed`) = '#{Current_date}')
          AND `p`.`patient_id` IN(#{patient_ids.join(',')})
        )
    group by `p`.`patient_id`
  ").collect do |p|
    { 
      :patient_id => p["patient_id"].to_i, :date_enrolled => p['date_enrolled'], 
      :earliest_start_date => p['earliest_start_date'], :death_date => (p['death_date'].to_date rescue nil), :age_at_initiation => p['age_at_initiation'], :age_in_days => p['age_in_days']
    } 
  end
  return records
end

start
