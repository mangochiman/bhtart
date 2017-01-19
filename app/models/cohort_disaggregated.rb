class CohortDisaggregated

  def self.get_indicators(start_date, end_date)
  time_started = Time.now().strftime('%Y-%m-%d %H:%M:%S')
  #######################################################################################################


#=end
      #Get earliest date enrolled
      cum_start_date = CohortRevise.get_cum_start_date
      cohort = CohortService.new(cum_start_date)

      #Total registered
      cohort.total_registered = CohortRevise.total_registered(start_date, end_date)
      cohort.cum_total_registered = CohortRevise.total_registered(cum_start_date, end_date)

      #Patients initiated on ART first time
      cohort.initiated_on_art_first_time = CohortRevise.initiated_on_art_first_time(start_date, end_date)
      cohort.cum_initiated_on_art_first_time = CohortRevise.initiated_on_art_first_time(cum_start_date, end_date)

      #Patients re-initiated on ART
      cohort.re_initiated_on_art = CohortRevise.re_initiated_on_art(start_date, end_date)
      cohort.cum_re_initiated_on_art = CohortRevise.re_initiated_on_art(cum_start_date, end_date)

      #Patients transferred in on ART
      cohort.transfer_in = CohortRevise.transfer_in(start_date, end_date)
      cohort.cum_transfer_in = CohortRevise.transfer_in(cum_start_date, end_date)


      #All males
      cohort.all_males = CohortRevise.males(start_date, end_date)
      cohort.cum_all_males = CohortRevise.males(cum_start_date, end_date)

      #Pregnant females (all ages)
=begin
Unique PatientProgram entries at the current location for those patients with at least one state ON ARVs and earliest start date of the 'ON ARVs' state within the quarter and having gender of related PERSON entry as F for female and 'IS PATIENT PREGNANT?' observation answered 'YES' in related HIV CLINIC CONSULTATION encounters within 28 days from earliest registration date OR in HIV Staging encounters
=end
      cohort.pregnant_females_all_ages = CohortRevise.pregnant_females_all_ages(start_date, end_date)
      cohort.cum_pregnant_females_all_ages = CohortRevise.pregnant_females_all_ages(cum_start_date, end_date)

      #Non-pregnant females (all ages)
=begin
      Unique PatientProgram entries at the current location for those patients with at least one state ON ARVs
      and earliest start date of the 'ON ARVs' state within the quarter and having gender of
      related PERSON entry as F for female and no entries of 'IS PATIENT PREGNANT?' observation answered 'YES'
      in related HIV CLINIC CONSULTATION encounters not within 28 days from earliest registration date
=end
      cohort.non_pregnant_females = CohortRevise.non_pregnant_females(start_date, end_date, cohort.pregnant_females_all_ages)
      cohort.cum_non_pregnant_females = CohortRevise.non_pregnant_females(cum_start_date, end_date, cohort.cum_pregnant_females_all_ages)

      #Unknown age
      cohort.unknown_age = CohortRevise.unknown_age(start_date, end_date)
      cohort.cum_unknown_age = CohortRevise.unknown_age(cum_start_date, end_date)

=begin
    Breastfeeding mothers

    Unique PatientProgram entries at the current location for those patients with at least one state
    ON ARVs and earliest start date of the 'ON ARVs' state within the quarter
    and having a REASON FOR ELIGIBILITY observation with an answer as BREASTFEEDING
=end
    cohort.breastfeeding_mothers = CohortRevise.breastfeeding_mothers(start_date, end_date)
    cohort.cum_breastfeeding_mothers = CohortRevise.breastfeeding_mothers(cum_start_date, end_date)

=begin
  Pregnant women

  Unique PatientProgram entries at the current location for those patients with at least one state ON ARVs
  and earliest start date of the 'ON ARVs' state within the quarter
  and having a REASON FOR ELIGIBILITY observation with an answer as PATIENT PREGNANT
=end
    cohort.pregnant_women = CohortRevise.pregnant_women(start_date, end_date)
    cohort.cum_pregnant_women = CohortRevise.pregnant_women(cum_start_date, end_date)

=begin
    No TB
    total_registered - (current_episode - tb_within_the_last_two_years)
=end
    cohort.no_tb = CohortRevise.no_tb(cohort.total_registered, cohort.tb_within_the_last_two_years, cohort.current_episode_of_tb)
    cohort.cum_no_tb = CohortRevise.cum_no_tb(cohort.cum_total_registered, cohort.cum_tb_within_the_last_two_years, cohort.cum_current_episode_of_tb)

=begin
    From this point going down: we update temp_earliest_start_date cum_outcome field to have the latest Cumulative outcome
=end
    CohortRevise.update_cum_outcome(end_date)


=begin
    Total Alive and On ART
    Unique PatientProgram entries at the current location for those patients with at least one state
    ON ARVs and earliest start date of the 'ON ARVs' state less than or equal to end date of quarter
    and latest state is ON ARVs  (Excluding defaulters)
=end
    cohort.total_alive_and_on_art = CohortRevise.get_outcome('On antiretrovirals')


    puts "Started at: #{time_started}. Finished at: #{Time.now().strftime('%Y-%m-%d %H:%M:%S')}"
    return cohort

  end

  def self.get_data(start_date, end_date, gender, age_group)

    @@cohort = get_indicators(start_date, end_date)

    if gender == 'Male' || gender == 'Female'
      if age_group == '50+ years' 
        yrs_months = 'year' ; age_to = 1000 ; age_from = 50
      elsif age_group.match(/years/i)
        age_from, age_to = age_group.sub(' years','').split('-')
        yrs_months = 'year'
      elsif age_group.match(/months/i)
        age_from, age_to = age_group.sub(' months','').split('-')
        yrs_months = 'month'
      end

      gender = gender.first
    
      started_on_art = self.get_started_on_art(yrs_months, age_from, age_to, gender, start_date, end_date)
      alive_on_art = self.get_alive_on_art(yrs_months, age_from, age_to, gender, start_date, end_date)
      started_on_ipt = self.get_started_on_ipt(yrs_months, age_from, age_to, gender, start_date, end_date)
      screened_for_tb = self.get_screened_for_tb(yrs_months, age_from, age_to, gender, start_date, end_date)

      return [started_on_art, alive_on_art, started_on_ipt, screened_for_tb]
    end 

    if gender == 'M'
      age_from = 0  ; age_to = 1000 ; yrs_months = 'year'
      started_on_art = self.get_started_on_art(yrs_months, age_from, age_to, gender, start_date, end_date)
      alive_on_art = self.get_alive_on_art(yrs_months, age_from, age_to, gender, start_date, end_date)
      started_on_ipt = self.get_started_on_ipt(yrs_months, age_from, age_to, gender, start_date, end_date)
      screened_for_tb = self.get_screened_for_tb(yrs_months, age_from, age_to, gender, start_date, end_date)

      return [started_on_art, alive_on_art, started_on_ipt, screened_for_tb]
    end

    return [0, 0, 0, 0]
  end 

  def self.get_screened_for_tb(yrs_months, age_from, age_to, gender, start_date, end_date)
    alive_on_art_patient_ids = []

    (@@cohort.total_alive_and_on_art || []).each do |data|
      alive_on_art_patient_ids << data['patient_id'].to_i
    end

    return 0 if alive_on_art_patient_ids.blank?

    tb_treatment = ConceptName.find_by_name('TB treatment').concept_id
    clinical_consultation = EncounterType.find_by_name('HIV CLINIC CONSULTATION').id

    data = ActiveRecord::Base.connection.select_all <<EOF
    SELECT t1.patient_id FROM obs t3
    INNER JOIN temp_earliest_start_date t1 ON t1.patient_id = t3.person_id
    WHERE t3.concept_id = #{tb_treatment} AND t3.voided = 0
    AND t3.obs_datetime BETWEEN '#{start_date.to_date.strftime('%Y-%m-%d 00:00:00')}' 
    AND '#{end_date.to_date.strftime('%Y-%m-%d 23:59:59')}' 
    AND gender = '#{gender.first}' AND t1.date_enrolled BETWEEN 
    '#{start_date.to_date}' AND '#{end_date.to_date}' 
    AND timestampdiff(#{yrs_months}, birthdate, DATE('#{end_date.to_date}')) 
    BETWEEN #{age_from} AND #{age_to} AND t1.patient_id IN(#{alive_on_art_patient_ids.join(',')})
    AND t3.obs_datetime = (

      SELECT MAX(obs_datetime) FROM obs t4 
      INNER JOIN encounter e ON e.encounter_id = t4.encounter_id
      AND e.encounter_type = #{clinical_consultation}
      WHERE t3.person_id = t4.person_id
      AND t4.voided = 0 AND t4.obs_datetime BETWEEN '#{start_date.to_date.strftime('%Y-%m-%d 00:00:00')}' 
      AND '#{end_date.to_date.strftime('%Y-%m-%d 23:59:59')}'

    )
    GROUP BY t3.person_id;
EOF

    return data.length rescue 0

  end



  def self.get_started_on_ipt(yrs_months, age_from, age_to, gender, start_date, end_date)

    data = ActiveRecord::Base.connection.select_all <<EOF
    SELECT patient_id FROM temp_earliest_start_date
    WHERE gender = '#{gender}' AND earliest_start_date <= '#{end_date.to_date}'
    AND timestampdiff(#{yrs_months}, birthdate, DATE('#{end_date.to_date}')) 
    BETWEEN #{age_from} AND #{age_to};
EOF

    return 0 if data.blank?

    patient_ids = []

    (data).each do |d|
      patient_ids << d['patient_id'].to_i
    end

    amount_dispensed = ConceptName.find_by_name('Amount dispensed').concept_id
    ipt_drug_ids = Drug.find_all_by_concept_id(ConceptName.find_by_name('Isoniazid').concept_id).map(&:drug_id)

    data = ActiveRecord::Base.connection.select_all <<EOF
    SELECT obs.person_id FROM obs
    WHERE concept_id = #{amount_dispensed} AND obs.voided = 0
    AND obs.obs_datetime <= '#{end_date.to_date.strftime('%Y-%m-%d 23:59:59')}' 
    AND value_drug IN(#{ipt_drug_ids.join(',')}) AND obs.person_id IN(#{patient_ids.join(',')});
EOF

    return data.length rescue 0
  end

  def self.get_alive_on_art(yrs_months, age_from, age_to, gender, start_date, end_date)
    alive_on_art_patient_ids = []

    (@@cohort.total_alive_and_on_art || []).each do |data|
      alive_on_art_patient_ids << data['patient_id'].to_i
    end

    return 0 if alive_on_art_patient_ids.blank?

    data = ActiveRecord::Base.connection.select_all <<EOF
    SELECT patient_id FROM temp_earliest_start_date
    WHERE gender = '#{gender}' AND earliest_start_date <= '#{end_date.to_date}' AND
    patient_id IN(#{alive_on_art_patient_ids.join(',')})
    AND timestampdiff(#{yrs_months}, birthdate, DATE('#{end_date.to_date}')) 
    BETWEEN #{age_from} AND #{age_to};
EOF

    return data.length rescue 0

  end

  def self.get_started_on_art(yrs_months, age_from, age_to, gender, start_date, end_date)
    alive_on_art_patient_ids = []

    (@@cohort.total_alive_and_on_art || []).each do |data|
      alive_on_art_patient_ids << data['patient_id'].to_i
    end

    return 0 if alive_on_art_patient_ids.blank?

    data = ActiveRecord::Base.connection.select_all <<EOF
    SELECT patient_id FROM temp_earliest_start_date
    WHERE gender = '#{gender}' AND earliest_start_date BETWEEN
    '#{start_date.to_date}' AND '#{end_date.to_date}' AND
    patient_id IN(#{alive_on_art_patient_ids.join(',')})
    AND timestampdiff(#{yrs_months}, birthdate, DATE('#{end_date.to_date}')) 
    BETWEEN #{age_from} AND #{age_to}
EOF

    return data.length rescue 0

  end


=begin    
    if ag == '50+ years'
      diff = [50, 1000]
      iu = 'year'
    elsif ag.match(/years/i)
      diff = ag.sub(' years','').split('-')
      iu = 'year'
    elsif ag.match(/months/i)
      diff = ag.sub(' months','').split('-')
      iu = 'month'
    else
      #############################################
      return self.get_disaggregated_cohort_all(start_date, end_date, gender, ag)
      ###################################
    end
return [(data.length rescue 0), (data1.length rescue 0),
      (data2.length rescue 0), (data3.length rescue 0)]
=end





end
