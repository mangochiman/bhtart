class ValidationRule < ActiveRecord::Base
  has_many :validation_results
  @dispensed_id = ConceptName.find_by_name('PILLS DISPENSED').concept_id

  def self.rules_xy
    rules = []

    self.find_by_sql("SELECT * FROM validation_rules").each do |rule|
      rules << rule.expr.scan(/\{\w+\}/).collect{|xpr| xpr.gsub(/\{|\}/, "")}
    end
    rules.flatten.uniq
  end

  def self.data_consistency_checks(date = Date.today)
    date = date.to_date
    require 'colorize'
    data_consistency_checks = {}
    #All methods for now should be here:
    data_consistency_checks['Patients without outcomes'] = "self.patients_without_outcomes(date)"
    data_consistency_checks['Patients with pills remaining greater than dispensed'] = "self.pills_remaining_over_dispensed(date)"
    data_consistency_checks['Patients without reason for starting'] = "self.validate_presence_of_start_reason(date)"
    data_consistency_checks['Patients with missing dispensations'] = "self.prescrition_without_dispensation(date)"
    data_consistency_checks['Patients with missing prescriptions'] = "self.dispensation_without_prescription(date)"
    data_consistency_checks['Patients with dispensation without appointment'] = "self.dispensation_without_appointment(date)"
    data_consistency_checks['Patients with vitals without weight'] = "self.validate_presence_of_vitals_without_weight(date)"
    data_consistency_checks['Patients with encounters before birth or after death'] = "self.death_date_less_than_last_encounter_date_and_less_than_date_of_birth(date)"
    data_consistency_checks['Patients with encounters without obs or orders'] = "self.encounters_without_obs_or_orders(date)"
    data_consistency_checks['Patients with ART start date before birth'] = "self.start_date_before_birth(date)"
    data_consistency_checks['Dead patients with follow up visits'] = "self.visit_after_death(date)"
    data_consistency_checks['Male patients with pregnant observations'] = "self.male_patients_with_pregnant_observation(date)"
    data_consistency_checks['Male patients with breastfeeding observations'] = "self.male_patients_with_breastfeeding_obs(date)"
    data_consistency_checks['Male patients with family planning methods obs'] = "self.male_patients_with_family_planning_methods_obs(date)"
    data_consistency_checks['ART patients without HIV clinic registration encounter'] = "self.check_every_ART_patient_has_HIV_Clinical_Registration(date)"
    data_consistency_checks['Under 18 patients without height and weight in visit'] = "self.every_visit_of_patients_who_are_under_18_should_have_height_and_weight(date)"
    data_consistency_checks['Patients with outcomes without date'] = "self.every_outcome_needs_a_date(date)"
    data_consistency_checks['Patients without gender'] = "self.patients_without_gender(date)"
    data_consistency_checks['Pre-ART patients with ARV drugs'] = "self.pre_art_patients_with_arv_drugs(date)"
    data_consistency_checks['Patients with treatment encounters without orders'] = "self.patients_with_treatment_encounter_without_orders(date)"
    data_consistency_checks['Patients with encounters without observations'] = "self.patients_with_encounters_without_observations(date)"
    data_consistency_checks['Patients without birthdate'] = "self.patients_without_birthdate(date)"
    data_consistency_checks['Patients with first visit greater than birthdate'] = "self.first_visit_date_greater_than_birthdate(date)"
    data_consistency_checks['Patients with death_date less than last visit date'] = "self.death_date_less_than_last_visit_date(date)"
		
    data_consistency_checks = data_consistency_checks.keys.inject({}){|hash, key| 
      time = Time.now
      puts "Running query for #{key}"
      hash[key] = eval(data_consistency_checks[key])
      period = (Time.now - time).to_i
     
      color = hash[key].length > 0 ? "red" : "green"
      eval("puts 'Time taken  :  #{(period/60).to_i} min  and #{(period % 60)} sec  --> #{hash[key].length} patient(s) found'.#{color}")
      puts ""	
      hash}
		
      set_rules = self.find(:all,:conditions =>['type_id = 2'])                   
      (set_rules || []).each do |rule|                                            
         unless data_consistency_checks[rule.desc].blank?                          
           create_update_validation_result(rule, date, data_consistency_checks[rule.desc])
      end                                                                       
    end                                                                         
                                                                                
    return data_consistency_checks
  end

  def self.create_update_validation_result(rule, date, patient_ids)
    date_checked = date.to_date                                                 
    v = ValidationResult.find(:first,                                           
      :conditions =>["date_checked = ? AND rule_id = ?", date_checked,rule.id]) 

    return ValidationResult.create(:rule_id => rule.id, :failures => patient_ids.length,
      :date_checked => date_checked) if v.blank?                                
                                                                                
    v.failures = patient_ids.length                                             
    v.save
  end

  def self.patients_without_outcomes(visit_date)

    visit_date = visit_date.to_date
    connection = ActiveRecord::Base.connection
    art_patient_ids = connection.select_all("SELECT patient_id FROM flat_cohort_table
      WHERE DATE(earliest_start_date) <= '#{visit_date}'").collect{|p|p["patient_id"]}

    patient_ids = connection.select_all("SELECT patient_id FROM flat_table2 WHERE
      DATE(visit_date) <= '#{visit_date}' AND patient_id in (#{art_patient_ids.join(',')})
      AND current_hiv_program_state IS NULL OR current_hiv_program_state = ''").collect{|p|p["patient_id"]}
    
   return patient_ids
=begin
    visit_date = visit_date.to_date rescue Date.today
    connection = ActiveRecord::Base.connection
    patient_ids = []
    without_outcome_ids = connection.select_all("
        SELECT e.patient_id as patient_id FROM encounter e INNER JOIN patient p
        ON e.patient_id=p.patient_id INNER JOIN patient_program pp ON p.patient_id=pp.patient_id
        LEFT JOIN patient_state ps ON pp.patient_program_id=ps.patient_program_id
        WHERE ps.patient_state_id IS NULL AND (e.voided=0 AND pp.voided=0 OR ps.voided=0)
        AND DATE(e.encounter_datetime) <= \'#{visit_date}\'
        GROUP BY patient_id

      ")
    
    without_outcome_ids.each do |pid|
      patient_ids << pid["patient_id"]
    end
    return patient_ids
=end
  end

  def self.pills_remaining_over_dispensed(visit_date=Date.today)
=begin
    visit_date = visit_date.to_date
    connection = ActiveRecord::Base.connection
    art_patient_ids = connection.select_all("SELECT patient_id FROM flat_cohort_table
      WHERE DATE(earliest_start_date) <= '#{visit_date}'").collect{|p|p["patient_id"]}

    amount_of_drug_one_dispensed_query = "SELECT COALESCE(
        (SELECT COALESCE(drug_quantity1, 0) FROM flat_table2 WHERE
        patient_id = patientID AND drug_quantity1 IS NOT NULL AND
        DATE(visit_date) < DATE(visitDate)
        ORDER BY DATE(visit_date) DESC LIMIT 1), 0)"

    amount_of_drug_two_dispensed_query = "SELECT COALESCE(
        (SELECT COALESCE(drug_quantity2, 0) FROM flat_table2 WHERE
        patient_id = patientID AND drug_quantity2 IS NOT NULL AND
        DATE(visit_date) < DATE(visitDate)
        ORDER BY DATE(visit_date) DESC LIMIT 1), 0)"

    amount_of_drug_three_dispensed_query = "SELECT COALESCE(
        (SELECT COALESCE(drug_quantity3, 0) FROM flat_table2 WHERE
        patient_id = patientID AND drug_quantity3 IS NOT NULL AND
        DATE(visit_date) < DATE(visitDate)
        ORDER BY DATE(visit_date) DESC LIMIT 1), 0)"

    amount_of_drug_four_dispensed_query = "SELECT COALESCE(
        (SELECT COALESCE(drug_quantity4, 0) FROM flat_table2 WHERE
        patient_id = patientID AND drug_quantity4 IS NOT NULL AND
        DATE(visit_date) < DATE(visitDate)
        ORDER BY DATE(visit_date) DESC LIMIT 1), 0)"

    amount_of_drug_five_dispensed_query = "SELECT COALESCE(
        (SELECT COALESCE(drug_quantity5, 0) FROM flat_table2 WHERE
        patient_id = patientID AND drug_quantity5 IS NOT NULL AND
        DATE(visit_date) < DATE(visitDate)
        ORDER BY DATE(visit_date) DESC LIMIT 1), 0)"

    patient_drug_details = connection.select_all("SELECT patient_id as patientID,
      visit_date as visitDate,
      (#{amount_of_drug_one_dispensed_query}) as amount_of_drug_one_dispensed,
      (#{amount_of_drug_two_dispensed_query}) as amount_of_drug_two_dispensed,
      (#{amount_of_drug_three_dispensed_query}) as amount_of_drug_three_dispensed,
      (#{amount_of_drug_four_dispensed_query}) as amount_of_drug_four_dispensed,
      (#{amount_of_drug_five_dispensed_query}) as amount_of_drug_five_dispensed,
      COALESCE(amount_of_drug1_brought_to_clinic,0) as amount_of_drug_one_brought,
      COALESCE(amount_of_drug2_brought_to_clinic,0) as amount_of_drug_two_brought,
      COALESCE(amount_of_drug3_brought_to_clinic,0) as amount_of_drug_three_brought,
      COALESCE(amount_of_drug4_brought_to_clinic,0) as amount_of_drug_four_brought,
      COALESCE(amount_of_drug5_brought_to_clinic,0) as amount_of_drug_five_brought
      FROM flat_table2 WHERE DATE(visit_date) <= '#{visit_date}' AND
      patient_id in (#{art_patient_ids.join(',')})
      AND (
           amount_of_drug1_brought_to_clinic IS NOT NULL OR
           amount_of_drug2_brought_to_clinic IS NOT NULL OR
           amount_of_drug3_brought_to_clinic IS NOT NULL OR
           amount_of_drug4_brought_to_clinic IS NOT NULL OR
           amount_of_drug5_brought_to_clinic IS NOT NULL
          )
      ORDER BY (DATE(visit_date)) DESC")
    patient_ids = []
    patient_drug_details.each do |data|
      amount_of_drug_one_dispensed = data["amount_of_drug_one_dispensed"].to_i
      amount_of_drug_two_dispensed = data["amount_of_drug_two_dispensed"].to_i
      amount_of_drug_three_dispensed = data["amount_of_drug_three_dispensed"].to_i
      amount_of_drug_four_dispensed = data["amount_of_drug_four_dispensed"].to_i
      amount_of_drug_five_dispensed = data["amount_of_drug_one_dispensed"].to_i

      amount_of_drug_one_brought = data["amount_of_drug_one_brought"].to_i
      amount_of_drug_two_brought = data["amount_of_drug_two_brought"].to_i
      amount_of_drug_three_brought = data["amount_of_drug_three_brought"].to_i
      amount_of_drug_four_brought = data["amount_of_drug_four_brought"].to_i
      amount_of_drug_five_brought = data["amount_of_drug_five_brought"].to_i
      if (
          (amount_of_drug_one_brought > amount_of_drug_one_dispensed) ||
          (amount_of_drug_two_brought > amount_of_drug_two_dispensed) ||
          (amount_of_drug_three_brought > amount_of_drug_three_dispensed) ||
          (amount_of_drug_four_brought > amount_of_drug_four_dispensed) ||
          (amount_of_drug_five_brought > amount_of_drug_five_dispensed)
         )
         patient_ids << data["patientID"]
      end
      
    end
    
    return patient_ids
=end

    visit_date = visit_date.to_date rescue Date.today
    patient_ids = []
    #art_adherence_enc = EncounterType.find_by_name('ART ADHERENCE').id
    dispensing_enc = EncounterType.find_by_name('DISPENSING').id
    #amount_dispensed_concept = Concept.find_by_name('AMOUNT DISPENSED').id
    amount_brought_to_clinic_concept = Concept.find_by_name('AMOUNT OF DRUG BROUGHT TO CLINIC').id
    
    patients = Patient.find_by_sql("
      SELECT ord.order_id as orderID, enc.patient_id as patient_ID,
      do.quantity as amought_dispensed from encounter enc INNER JOIN obs ON
      enc.encounter_id=obs.encounter_id INNER JOIN orders ord ON ord.order_id=obs.order_id
      AND enc.encounter_type = #{dispensing_enc} INNER JOIN drug_order do ON ord.order_id = do.order_id
      AND enc.voided=0 WHERE do.quantity > 0 AND DATE(enc.encounter_datetime) <= \'#{visit_date}\'
      HAVING  amought_dispensed < (SELECT o.value_numeric  FROM obs o
      WHERE order_id = orderID AND o.concept_id=#{amount_brought_to_clinic_concept} LIMIT 1)
      ")
    patient_ids = patients.collect{|patient|patient["patient_ID"]}

    return patient_ids
  end
  
  def self.validate_presence_of_start_reason(end_date = Date.today)
    #This function checks for patients who do not have a reason for starting ART in flat tables

    patients = Patient.find_by_sql("SELECT patient_id FROM flat_cohort_table WHERE earliest_start_date <= '#{end_date}'
                                    AND COALESCE(TRIM(reason_for_starting), '') = ''").collect{|x| x.patient_id}

=begin
    #This part was commented out because it deals with the validation in the main db
    start_reason_concept = Concept.find_by_name("Reason for art eligibility").id

    patient_ids = PatientProgram.find_by_sql("SELECT patient_id FROM earliest_start_date
                where earliest_start_date <= '#{end_date}' and patient_id NOT IN
                (SELECT distinct person_id from obs where concept_id = #{start_reason_concept} and voided = 0)").map(&:patient_id)

    return patient_ids
=end
  end

  def self.dispensation_without_prescription(end_date = Date.today)

    visit_date = end_date.to_date
    connection = ActiveRecord::Base.connection
    art_patient_ids = connection.select_all("SELECT patient_id FROM flat_cohort_table
      WHERE DATE(earliest_start_date) <= '#{visit_date}'").collect{|p|p["patient_id"]}

    patient_ids = connection.select_all("SELECT patient_id FROM flat_table2 WHERE
      DATE(visit_date) <= '#{visit_date}' AND patient_id in (#{art_patient_ids.join(',')})
      AND (
            ((COALESCE(drug_quantity1,0) > 0) AND drug_encounter_id1 IS NULL) OR
            ((COALESCE(drug_quantity2,0) > 0) AND drug_encounter_id2 IS NULL) OR
            ((COALESCE(drug_quantity3,0) > 0) AND drug_encounter_id3 IS NULL) OR
            ((COALESCE(drug_quantity4,0) > 0) AND drug_encounter_id4 IS NULL) OR
            ((COALESCE(drug_quantity5,0) > 0) AND drug_encounter_id5 IS NULL)
          )"
    ).collect{|p|p["patient_id"]}

    return patient_ids
=begin
    unprescribed = Observation.find_by_sql("
                                  SELECT DISTINCT(person_id)  FROM obs
                                  WHERE (order_id <=> NULL)
                                  AND concept_id = #{@dispensed_id}
                                  AND DATE(obs_datetime) <= '#{end_date}'
                                  AND voided = 0").map(&:patient_id)
    return unprescribed
=end

  end

  def self.prescrition_without_dispensation(end_date = Date.today)
    visit_date = end_date.to_date
    connection = ActiveRecord::Base.connection
    art_patient_ids = connection.select_all("SELECT patient_id FROM flat_cohort_table
      WHERE DATE(earliest_start_date) <= '#{visit_date}'").collect{|p|p["patient_id"]}

    patient_ids = connection.select_all("SELECT patient_id FROM flat_table2 WHERE
      DATE(visit_date) <= '#{visit_date}' AND patient_id in (#{art_patient_ids.join(',')})
      AND (
            (drug_encounter_id1 IS NOT NULL AND (COALESCE(drug_quantity1,0) = 0)) OR
            (drug_encounter_id2 IS NOT NULL AND (COALESCE(drug_quantity2,0) = 0)) OR
            (drug_encounter_id3 IS NOT NULL AND (COALESCE(drug_quantity3,0) = 0)) OR
            (drug_encounter_id4 IS NOT NULL AND (COALESCE(drug_quantity4,0) = 0)) OR
            (drug_encounter_id5 IS NOT NULL AND (COALESCE(drug_quantity5,0) = 0))
          )"
    ).collect{|p|p["patient_id"]}

    return patient_ids

=begin
    undispensed = Order.find_by_sql("
                                    SELECT DISTINCT(patient_id) FROM orders
                                    WHERE NOT EXISTS (SELECT order_id FROM obs WHERE order_id = orders.order_id
                                    AND concept_id = #{@dispensed_id} and  voided = 0)
                                    AND DATE(start_date)  <= '#{end_date}'
                                    AND orders.voided = 0").map(&:patient_id)
    return undispensed
=end
  end

  def self.dispensation_without_appointment(end_date = Date.today)

    visit_date = end_date.to_date
    connection = ActiveRecord::Base.connection
    art_patient_ids = connection.select_all("SELECT patient_id FROM flat_cohort_table
      WHERE DATE(earliest_start_date) <= '#{visit_date}'").collect{|p|p["patient_id"]}

    patient_ids = connection.select_all("SELECT patient_id FROM flat_table2 WHERE
      DATE(visit_date) <= '#{visit_date}' AND patient_id in (#{art_patient_ids.join(',')})
      AND (
            (drug_encounter_id1 IS NOT NULL AND (COALESCE(drug_quantity1,0) > 0) AND appointment_date IS NULL) OR
            (drug_encounter_id2 IS NOT NULL AND (COALESCE(drug_quantity2,0) > 0) AND appointment_date IS NULL) OR
            (drug_encounter_id3 IS NOT NULL AND (COALESCE(drug_quantity3,0) > 0) AND appointment_date IS NULL) OR
            (drug_encounter_id4 IS NOT NULL AND (COALESCE(drug_quantity4,0) > 0) AND appointment_date IS NULL) OR
            (drug_encounter_id5 IS NOT NULL AND (COALESCE(drug_quantity5,0) > 0) AND appointment_date IS NULL)
          )"
    ).collect{|p|p["patient_id"]}

  return patient_ids
=begin
    no_appointment = Observation.find_by_sql("
                                    SELECT DISTINCT(person_id) FROM obs
                                    WHERE concept_id = #{@dispensed_id}
                                    AND voided = 0
                                    AND DATE(obs_datetime) <= '#{end_date}'
                                    AND person_id NOT IN
                                    (SELECT person_id FROM obs o
                                    INNER JOIN encounter e ON o.person_id = e.patient_id
                                    INNER JOIN encounter_type et ON et.encounter_type_id = e.encounter_type
                                    WHERE et.name = 'Appointment'
                                    AND o.obs_datetime = obs_datetime
                                    AND o.person_id = person_id
                                    AND o.voided = 0)").map(&:person_id)
    return no_appointment
=end

  end

  def self.validate_presence_of_vitals_without_weight(date = Date.today)
    # Developer   : Kenneth Kapundi
    # Date        : 3/09/2014 
    # Purpose     : Return Patient IDs for patients having Vitals encounters without weight 
    # Amendments  :

    enc_ids = ["Height_enc_id", "height_for_age_enc_id", "Height",
                   "weight_for_height_enc_id", "weight_for_age_enc_id",
                   "Temperature_enc_id", "BMI_enc_id",
                   "systolic_blood_pressure", "diastolic_blood_pressure",
               ]
    return FlatTable2.find_by_sql(["SELECT DISTINCT(ft2.patient_id) FROM flat_table2 ft2
                  JOIN flat_cohort_table fct
                    ON ft2.patient_id = fct.patient_id
                  WHERE COALESCE(#{enc_ids.join(',')}) IS NOT NULL
                    AND (Weight IS NULL OR Weight = '') AND DATE(ft2.visit_date) = ?", date.to_date]).map(&:patient_id)
=begin

    weight_concept = ConceptName.find_by_name('weight').concept_id
    encounter_type = EncounterType.find_by_name('vitals').id

    patient_ids = ValidationRule.find_by_sql("SELECT DISTINCT e.patient_id 
                          FROM encounter e 
                              LEFT JOIN obs o ON e.encounter_id = o.encounter_id AND o.concept_id = #{weight_concept} AND o.voided = 0
                               WHERE o.concept_id IS NULL AND e.voided = 0 AND e.encounter_type = #{encounter_type} 
                               AND e.encounter_datetime <= '#{end_date}'").map(&:patient_id) 
=end

  end

  def self.death_date_less_than_last_visit_date(end_date = Date.today)
    #patients with visits after death    

    return ValidationRule.find_by_sql("SELECT 
				    ft2.patient_id,
				    fct.earliest_start_date,
				    fct.birthdate,
				    fct.death_date,
				    ft2.visit_date,
				    DATEDIFF(ft2.visit_date, fct.death_date)
				  FROM
    				    flat_table2 ft2
				        INNER JOIN
				    flat_cohort_table fct ON ft2.patient_id = fct.patient_id
				  WHERE DATEDIFF(ft2.visit_date, fct.death_date) > 0
				  AND DATE(ft2.visit_date) <= '#{end_date.to_date}'
				  GROUP BY ft2.patient_id").map(&:patient_id)
  end

  def self.first_visit_date_greater_than_birthdate(end_date = Date.today)
    #patients with visit_date before birth_date    

    return ValidationRule.find_by_sql("SELECT 
                                    ft2.patient_id,
                                    fct.earliest_start_date,
                                    fct.birthdate,
                                    fct.death_date,
                                    ft2.visit_date,
                                    DATEDIFF(fct.birthdate, ft2.visit_date)
                                  FROM
                                    flat_table2 ft2
                                        INNER JOIN
                                    flat_cohort_table fct ON ft2.patient_id = fct.patient_id
                                  WHERE DATEDIFF(fct.birthdate, ft2.visit_date) > 0
                                  AND DATE(ft2.visit_date) <= '#{end_date.to_date}'
                                  GROUP BY ft2.patient_id").map(&:patient_id)
  end

  def self.death_date_less_than_last_encounter_date_and_less_than_date_of_birth(date = Date.today)

    return FlatTable2.find_by_sql(["
			(SELECT DISTINCT (ft2.patient_id) FROM flat_table2 ft2
        INNER JOIN flat_cohort_table fct ON ft2.patient_id = fct.patient_id
			WHERE DATEDIFF(ft2.visit_date, fct.death_date) > 0
			  AND DATE(ft2.visit_date) <= ?)

       UNION
      (SELECT DISTINCT (ft2.patient_id) FROM flat_table2 ft2
        INNER JOIN flat_cohort_table fct ON ft2.patient_id = fct.patient_id
			WHERE DATEDIFF(fct.birthdate, ft2.visit_date) > 0
			  AND DATE(ft2.visit_date) <= ?)
    ", date.to_date, date.to_date]).map(&:patient_id).uniq

=begin
    #Task 41
    patient_ids =  ValidationRule.find_by_sql("SELECT DISTINCT(esd.patient_id)
					       FROM earliest_start_date esd
					         INNER JOIN person p ON p.person_id = esd.patient_id
					       WHERE p.birthdate IS NOT NULL 
					       AND esd.death_date IS NOT NULL 
					       AND esd.death_date < (SELECT MAX(encounter_datetime)
                       			                             FROM encounter e 
                       						     WHERE e.patient_id = esd.patient_id 
								     AND e.voided = 0) 
                                              AND (SELECT MAX(encounter_datetime)
                       		                   FROM encounter e 
                       				   WHERE e.patient_id = esd.patient_id 
						   AND e.voided = 0) < p.birthdate;").map(&:patient_id)
    return patient_ids

=end
  end

  def self.patients_with_treatment_encounter_without_orders(end_date = Date.today)
    #Query pulling all treatment encounters without orders
    ValidationRule.find_by_sql("SELECT e.patient_id, e.encounter_id, e.encounter_type, e.encounter_datetime
				FROM earliest_start_date esd
			          INNER JOIN encounter e ON e.patient_id = esd.patient_id AND e.voided = 0
				WHERE e.encounter_type IN (25)
				AND e.encounter_id NOT IN (SELECT encounter_id FROM orders WHERE voided = 0 AND DATE(start_date) <= '#{end_date.to_date}')
                                AND DATE(encounter_datetime) <= '#{end_date.to_date}'
				GROUP BY e.patient_id").map(&:patient_id)
  end 

  def self.patients_with_encounters_without_observations(end_date = Date.today)
   #Query pulling all encounters without observations 
   ValidationRule.find_by_sql("SELECT e.patient_id, e.encounter_id, e.encounter_type, e.encounter_datetime
			       FROM earliest_start_date esd
			         INNER JOIN encounter e ON e.patient_id = esd.patient_id AND e.voided = 0
			       WHERE e.encounter_id NOT IN (SELECT encounter_id FROM obs 
                                                            WHERE voided = 0 
                                                            AND DATE(obs_datetime) <= '#{end_date.to_date}')
                               AND DATE(encounter_datetime) <= '#{end_date.to_date}'
		               GROUP BY e.patient_id").map(&:patient_id)
  end

  def self.encounters_without_obs_or_orders(end_date = Date.today)
    #Query for encounters without obs or orders ~ Kenneth
    ValidationRule.find_by_sql(["
         			 SELECT DISTINCT (enc.patient_id) FROM encounter enc
    			           LEFT JOIN obs o ON o.encounter_id = enc.encounter_id
    			           LEFT JOIN orders od ON od.encounter_id = enc.encounter_id
			         WHERE enc.voided = 0 AND (o.encounter_id IS NULL OR o.voided = 1) AND (od.encounter_id IS NULL OR od.voided = 1)
			         AND DATE(enc.encounter_datetime) <= ?", end_date.to_date
			       ]).map(&:patient_id)		

  end
	
  def self.start_date_before_birth(date = Date.today)

    #begin Query for patients whose earliest start date is less that date of birth ~ Kenneth
    return FlatTable2.find_by_sql(["
			SELECT DISTINCT (ft2.patient_id) FROM flat_table2 ft2
        INNER JOIN flat_cohort_table fct ON ft2.patient_id = fct.patient_id
			WHERE DATEDIFF(fct.earliest_start_date, fct.birthdate) <= 0
			  AND DATE(ft2.visit_date) <= ?", date.to_date]).map(&:patient_id)

=begin Query for patients whose earliest start date is less that date of birth ~ Kenneth
		FlatTable1.find_by_sql(["
			SELECT DISTINCT (esd.patient_id) FROM flat_table1 esd
   				INNER JOIN person p ON p.person_id = esd.patient_id AND voided = 0
   				INNER JOIN encounter enc ON enc.patient_id = esd.patient_id
			WHERE DATEDIFF(esd.earliest_start_date, p.birthdate) <= 0
			AND enc.encounter_datetime BETWEEN ? AND ?", start_date, end_date
			]).map(&:patient_id)
=end

  end
	
  def self.visit_after_death(date = Date.today)

   # Query for patients with followup visit after death ~ Kenneth
   return FlatTable2.find_by_sql(["
			SELECT DISTINCT (ft2.patient_id) FROM flat_table2 ft2
        INNER JOIN flat_cohort_table fct ON ft2.patient_id = fct.patient_id
			WHERE DATEDIFF(ft2.visit_date, fct.death_date) > 0
			  AND DATE(ft2.visit_date) <= ?", date.to_date]).map(&:patient_id)

=begin	ValidationRule.find_by_sql(["
		SELECT DISTINCT(enc.patient_id) FROM person p
    		INNER JOIN encounter enc ON enc.patient_id = p.person_id
				AND enc.voided = 0 AND enc.encounter_datetime > p.death_date
    	WHERE p.dead = 1
			AND enc.encounter_datetime BETWEEN ? AND ?", start_date, end_date
			]).map(&:patient_id)
=end

  end	

  def self.male_patients_with_pregnant_observation(date = Date.today)

    pregnant_fields = [
                        "ft2.pregnant_yes", "ft2.pregnant_no", "ft2.pregnant_unknown",
                        "ft2.pregnant_yes_enc_id", "ft2.pregnant_no_enc_id", "ft2.pregnant_unknown_enc_id",
                        "ft2.pregnant_yes_v_date", "ft2.pregnant_no_v_date", "ft2.pregnant_unknown_v_date"
                      ]

    #Query pulling all male patients with pregnant observations

    male_pats_with_preg_obs = FlatTable2.find_by_sql(["SELECT ft2.patient_id FROM flat_table2 ft2
                                  INNER JOIN flat_cohort_table fct ON fct.patient_id = ft2.patient_id
                 WHERE fct.gender IN ('Male', 'M') AND COALESCE(#{pregnant_fields.join(',')}) IS NOT NULL
                                  AND DATE(ft2.visit_date) <= ?", date.to_date]).map(&:patient_id)

=begin
    male_pats_with_preg_obs = PatientProgram.find_by_sql("
                                SELECT esd.patient_id, p.gender,
                                       esd.earliest_start_date, o.concept_id,
                                       o.value_coded, o.obs_datetime
                                FROM earliest_start_date esd
	                                INNER JOIN person p ON p.person_id = esd.patient_id
	                                  AND p.voided = 0
                                  INNER JOIN obs o ON o.person_id = p.person_id
                                    AND o.voided = 0
                                WHERE p.gender = 'M'
                                AND (o.concept_id IN (#{pregnant_ids.join(',')})
                                  OR o.value_coded IN (#{pregnant_ids.join(',')}))
                                AND o.obs_datetime <= '#{@end_date}'
                                GROUP BY esd.patient_id").collect{|p| p.patient_id}
=end



    return male_pats_with_preg_obs
  end

  def self.male_patients_with_breastfeeding_obs(date = Date.today)


    breastfeeding_fields = [
        "ft2.breastfeeding_yes", "ft2.breastfeeding_no", "ft2.breastfeeding_unknown",
        "ft2.breastfeeding_yes_enc_id", "ft2.breastfeeding_no_enc_id", "ft2.breastfeeding_unknown_enc_id",
        "ft2.breastfeeding_yes_v_date", "ft2.breastfeeding_no_v_date", "ft2.breastfeeding_unknown_v_date"
    ]

    #Query pulling all male patients with breastfeeding observations
    male_pats_with_breastfeed_obs = FlatTable2.find_by_sql(["SELECT ft2.patient_id FROM flat_table2 ft2
                                  INNER JOIN flat_cohort_table fct ON fct.patient_id = ft2.patient_id
                   WHERE gender IN ('Male', 'M') AND COALESCE(#{breastfeeding_fields.join(',')}) IS NOT NULL
                                  AND DATE(ft2.visit_date) <= ?", date.to_date]).map(&:patient_id)
=begin
    male_pats_with_breastfeed_obs = PatientProgram.find_by_sql("
                                      SELECT esd.patient_id, p.gender,
                                             esd.earliest_start_date, o.concept_id,
                                             o.value_coded, o.obs_datetime
                                      FROM earliest_start_date esd
	                                      INNER JOIN person p ON p.person_id = esd.patient_id
	                                        AND p.voided = 0
                                        INNER JOIN obs o ON o.person_id = p.person_id
                                         AND o.voided = 0
                                      WHERE p.gender = 'M'
                                      AND (o.concept_id IN (#{breastfeeding_ids.join(',')})
                                        OR o.value_coded IN (#{breastfeeding_ids.join(',')}))
                                      AND o.obs_datetime <= '#{@end_date}'
                                      GROUP BY esd.patient_id").collect{|p| p.patient_id}
=end

    return male_pats_with_breastfeed_obs
  end

  def self.male_patients_with_family_planning_methods_obs(date = Date.today)

    family_planning_fields = FlatTable2.find_by_sql("SHOW COLUMNS FROM flat_table2 LIKE '%family_planning%'").map(&:Field)

    #Query pulling all male patients with family planning methods observations
    male_pats_with_family_planning_obs = FlatTable2.find_by_sql(["SELECT ft2.patient_id FROM flat_table2 ft2
                              INNER JOIN flat_cohort_table fct ON fct.patient_id = ft2.patient_id
                 WHERE fct.gender IN ('Male', 'M') AND COALESCE(#{family_planning_fields.join(',')}) IS NOT NULL
                              AND DATE(ft2.visit_date) <= ?", date.to_date]).map(&:patient_id)

=begin
    male_pats_with_family_planning_obs = PatientProgram.find_by_sql("
                                          SELECT esd.patient_id, p.gender,
                                                 esd.earliest_start_date, o.concept_id,
                                                 o.value_coded, o.obs_datetime
                                          FROM earliest_start_date esd
	                                          INNER JOIN person p ON p.person_id = esd.patient_id
	                                            AND p.voided = 0
                                            INNER JOIN obs o ON o.person_id = p.person_id
                                             AND o.voided = 0
                                          WHERE p.gender = 'M'
                               i           AND (o.concept_id IN (#{family_planing_ids.join(',')})
                                            OR o.value_coded IN (#{family_planing_ids.join(',')}))
                                          AND o.obs_datetime <= '#{@end_date}'
                                          GROUP BY esd.patient_id").collect{|p| p.patient_id}
=end

    return male_pats_with_family_planning_obs
  end

  def self.check_every_ART_patient_has_HIV_Clinical_Registration(date = Date.today)
			#Task 32
			#SQL to check for every ART patient should have a HIV Clinical Registration
			date = date.to_date.strftime('%Y-%m-%d 23:59:59')

      eligible_patients = Patient.find_by_sql("SELECT patient_id FROM flat_cohort_table").collect { |x| x.patient_id }
      
      return FlatCohortTable.find_by_sql("SELECT 
                                              fct.patient_id,
                                              fct.earliest_start_date,
                                              ft1.ever_received_art,
                                              ft1.type_of_confirmatory_hiv_test,
                                              ft1.confirmatory_hiv_test_location,
                                              ft1.ever_registered_at_art_clinic,
                                              ft1.agrees_to_followup
                                          FROM
                                              flat_cohort_table fct
                                                  INNER JOIN
                                              flat_table1 ft1 ON ft1.patient_id = fct.patient_id
                                          WHERE
                                              fct.earliest_start_date <= DATE('#{date}')
                                                  AND ft1.type_of_confirmatory_hiv_test IS NULL
                                                  AND ft1.confirmatory_hiv_test_location IS NULL
                                                  AND ft1.agrees_to_followup IS NULL
                                                  AND ft1.ever_received_art IS NULL
                                          GROUP BY fct.patient_id").map(&:patient_id)                                          
  
=begin
      return FlatTable1.find_by_sql("SELECT patient_id FROM flat_table1 WHERE patient_id in (#{eligible_patients.join(',')})
                                    AND (type_of_confirmatory_hiv_test IS NULL OR confirmatory_hiv_test_location IS NULL
                                    OR ever_received_art IS NULL OR agrees_to_followup IS NULL)
                                    AND earliest_start_date <= DATE('#{date}')").map(&:patient_id)


			encounter_type_id = EncounterType.find_by_name("HIV CLINIC REGISTRATION").encounter_type_id

			Patient.find_by_sql("
				SELECT p.patient_id
				FROM earliest_start_date p LEFT JOIN (SELECT * FROM encounter WHERE encounter_type = #{encounter_type_id}) e
						ON p.patient_id = e.patient_id
				WHERE e.encounter_type IS NULL AND p.earliest_start_date <= DATE('#{date}');
			").map(&:patient_id)
=end
	end

  def self.deliver_validation_results(rules_date = Date.today)
     sent_to_mail = {}
    ValidationResult.find_by_sql("
      SELECT * FROM validation_results vs
      INNER JOIN validation_rules vr ON vr.id = vs.rule_id
      ").each {|validated|
        sent_to_mail["#{validated.desc}"] = {}
        sent_to_mail["#{validated.desc}"]["failed"] = validated.failures
        sent_to_mail["#{validated.desc}"]["validated_on"] = validated.date_checked
      }
    return sent_to_mail
  end

  def self.every_visit_of_patients_who_are_under_18_should_have_height_and_weight(date = Date.today)
    #Task 31
    #SQL for every visit of patients who are under 18 should have height and weight

    date = date.to_date.strftime('%Y-%m-%d 23:59:59')

    eligible_patients = Patient.find_by_sql("SELECT patient_id, FLOOR(DATEDIFF(DATE('#{date}'), birthdate)/365) AS age
 FROM flat_cohort_table HAVING age < 18").collect { |x| x.patient_id }

    eligible_patients = [-10] if eligible_patients.blank? #to avoid mysql crash

    return Patient.find_by_sql("SELECT 
                                  ft2.visit_date,
                                  ftc.patient_id,
                                  ftc.birthdate,
                                  FLOOR(DATEDIFF(DATE(NOW()), ftc.birthdate) / 365.25) AS patient_age,
                                  ft2.guardian_present_yes,
	                                ft2.guardian_present_no,
                                  ft2.patient_present_no,
                                  ft2.patient_present_yes,
                                  ft2.weight,
                                  ft2.height
                              FROM
                                  flat_cohort_table ftc
                                      INNER JOIN
                                  flat_table2 ft2 ON ft2.patient_id = ftc.patient_id
                              WHERE
                                  (ft2.weight IS NULL
                                      OR ft2.height IS NULL) 
                              HAVING patient_age <= 18 AND ft2.patient_present_yes = 'Yes' 
                               AND (ft2.guardian_present_yes = 'Yes' Or ft2.guardian_present_no = 'No')").map(&:patient_id)

=begin
    return Patient.find_by_sql("SELECT patient_id FROM flat_table2 WHERE DATE(visit_date) <= DATE('#{date}') AND
                              patient_id in (#{eligible_patients.join(',')}) AND patient_present_yes = 'Yes'
                              AND (Weight IS NULL OR Height IS NULL)").map(&:patient_id)


		encounter_type_id = EncounterType.find_by_name("VITALS").encounter_type_id
		height_id = ConceptName.find_by_name("HT").concept_id
		weight_id = ConceptName.find_by_name("WT").concept_id

		Patient.find_by_sql("
			SELECT Weight_and_Height, patient_id, encounter_datetime, concept_id
			FROM(
					SELECT COUNT(*) AS Weight_and_Height, visit.* , e.encounter_type, o.concept_id, lue_numeric
						  FROM (
						      SELECT e.patient_id, DATE(e.encounter_datetime) AS encounter_datetime, birthdate,
						          FLOOR(DATEDIFF(DATE(e.encounter_datetime), birthdate)/365) AS age
						      FROM encounter e LEFT JOIN person p ON e.patient_id = p.person_id
						      WHERE e.voided = 0
						      GROUP BY e.patient_id, DATE(e.encounter_datetime)) visit
						  LEFT JOIN encounter e ON visit.patient_id = e.patient_id
						      AND visit.encounter_datetime = DATE(e.encounter_datetime)
						  LEFT JOIN obs o ON e.encounter_id=o.encounter_id
					WHERE age < 18 AND e.encounter_type = #{encounter_type_id} AND concept_id IN (#{height_id}, #{weight_id})
					GROUP BY visit.patient_id, visit.encounter_datetime) weight_and_height_check
			WHERE Weight_and_Height < 2  AND encounter_datetime = DATE('#{date}')").map(&:patient_id)
=end
	end

	def self.every_outcome_needs_a_date(date = Date.today)
          #Task 40
	  #Every outcome needs a date

          date = date.to_date.strftime('%Y-%m-%d 23:59:59')

          FlatTable2.find_by_sql("SELECT patient_id FROM flat_table2 WHERE COALESCE(TRIM(current_hiv_program_state),'') != ''
                                  AND DATE(current_hiv_program_start_date) IS NULL AND DATE(visit_date) <= DATE('#{date}')
                                  AND patient_id in (SELECT patient_id FROM flat_cohort_table)").map(&:patient_id)
=begin
           date = date.to_date.strftime('%Y-%m-%d 23:59:59')
           PatientState.find_by_sql("
	   SELECT pp.patient_id,p.patient_program_id, state, p.date_created
	   FROM patient_state p LEFT JOIN patient_program pp
	      ON p.patient_program_id = pp.patient_program_id
	   WHERE start_date IS NULL AND p.date_created <= '#{date}'").map(&:patient_id)
=end
  end
 
  def self.patients_without_gender(end_date = Date.today)
   #pulling out without gender
   ValidationRule.find_by_sql("SELECT fct.patient_id, fct.gender, fct.earliest_start_date
                               FROM flat_cohort_table fct
                               WHERE (fct.gender IS NULL OR fct.gender = '')
                               AND fct.earliest_start_date <= '#{end_date.to_date}'").map(&:patient_id)
  end
  
  def self.pre_art_patients_with_arv_drugs(end_date = Date.today)
   #pulling all pre-art patients with ARV drugs
   ValidationRule.find_by_sql("SELECT 
                                 fct.patient_id,
                                 fct.gender,
                                 fct.earliest_start_date,
                                 fct.hiv_program_state,
                                 fct.hiv_program_start_date
                               FROM
                                   flat_cohort_table fct
                                 INNER JOIN
                                   arv_drugs_orders ado ON ado.patient_id = fct.patient_id
                               WHERE
                                 fct.hiv_program_state = 'Pre-ART (Continue)'
                                 AND fct.earliest_start_date <= '#{end_date.to_date}'
                                 AND DATE(ado.start_date) <= '#{end_date.to_date}'
                               GROUP BY fct.patient_id").map(&:patient_id)
  end

  def self.patients_without_birthdate(end_date = Date.today)
    #pulling all patients without birthdate
    ValidationRule.find_by_sql("SELECT fct.patient_id, fct.birthdate, fct.earliest_start_date
                               FROM flat_cohort_table fct
                               WHERE (fct.birthdate IS NULL OR fct.birthdate = '' OR fct.birthdate = '0000-00-00')
                               AND fct.earliest_start_date <= '#{end_date.to_date}'").map(&:patient_id)

  end
 
  def self.patients_with_birthdate_less_than_death_date(end_date = Date.today)
    #pulling all patients with bithdate less than death_date
    ValidationRule.find_by_sql("SELECT fct.patient_id, fct.birthdate, fct.death_date, fct.earliest_start_date
                               FROM flat_cohort_table fct
                               WHERE Date(fct.birthdate) < DATE(fct.death_date)
                               AND fct.earliest_start_date <= '#{end_date.to_date}'").map(&:patient_id)

  end

  def self.patients_with_earliest_start_date_greater_than_first_received_drug_date(end_date = Date.today)
   #pulling all patients with earliest_start_date greater than first received drug date
   ValidationRule.find_by_sql("SELECT fct.patient_id, fct.earliest_start_date
			       FROM flat_cohort_table fct
			       WHERE fct.earliest_start_date < (SELECT min(DATE(start_date)) FROM amount_dispensed_obs
                               				         WHERE person_id = fct.patient_id
				                                 AND DATE(start_date) < '#{end_date.to_date}')
			       AND fct.earliest_start_date < '#{end_date.to_date}'
			       GROUP BY fct.patient_id").map(&:patient_id) 
  end
end
