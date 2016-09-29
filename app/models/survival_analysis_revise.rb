class SurvivalAnalysisRevise

  def self.get_indicators(start_date, end_date)
  time_started = Time.now().strftime('%Y-%m-%d %H:%M:%S')
=begin
    ActiveRecord::Base.connection.execute <<EOF
      DROP TABLE IF EXISTS `temp_earliest_start_date`;
EOF


    ActiveRecord::Base.connection.execute <<EOF
      CREATE TABLE temp_earliest_start_date
select
        `p`.`patient_id` AS `patient_id`,
        `p`.`gender` AS `gender`,
        `p`.`birthdate`,
        `p`.`earliest_start_date` AS `earliest_start_date`,
        cast(`pf`.`encounter_datetime` as date) AS `date_enrolled`,
        `p`.`death_date` AS `death_date`,
        (select timestampdiff(year, `p`.`birthdate`, `p`.`earliest_start_date`)) AS `age_at_initiation`,
        (select timestampdiff(day, `p`.`birthdate`, `p`.`earliest_start_date`)) AS `age_in_days`
    from
        (`patients_on_arvs` `p`
        join `patient_first_arv_amount_dispensed` `pf` ON ((`pf`.`patient_id` = `p`.`patient_id`)))
    group by `p`.`patient_id`
EOF
=end
    #Get earliest date enrolled
    cum_start_date = self.get_cum_start_date
    survival_start_date = start_date.to_date
    survival_end_date = end_date.to_date

    date_ranges = []
    cohort = CohortService.new(cum_start_date)

    while (survival_start_date -= 1.year) >= cum_start_date
      survival_end_date   -= 1.year
      date_ranges << {:start_date => survival_start_date,
                      :end_date   => survival_end_date
      }
    end

=begin
    The number of patients registered at ART within the 12 months
=end
    states = {}; women_outcomes = {}; children_outcomes = {}; pregnant_and_breast_feeding_women={};
    interval = ""
    date_ranges.sort_by {|x,i| x[:end_date] <=>  x[:start_date]}.each_with_index do |range, i|

      interval = "#{(i + 1)*12}"

      states[interval.to_i] = self.general_analysis(range[:start_date], range[:end_date])
      pregnant_and_breast_feeding_women[range[:start_date]] = self.pregnant_and_breast_feeding_women(range[:start_date], range[:end_date])
      children_outcomes[range[:start_date]] = self.children_analysis(range[:start_date], range[:end_date], 0, 14)
    end

    cohort.general_survival_analysis = states
    cohort.women_survival_analysis = pregnant_and_breast_feeding_women
    cohort.children_survival_analysis = children_outcomes

    puts "Started at: #{time_started}. Finished at: #{Time.now().strftime('%Y-%m-%d %H:%M:%S')}"

    return cohort
  end


  private
  def self.get_cum_start_date
    cum_start_date = ActiveRecord::Base.connection.select_value <<EOF
      SELECT MIN(date_enrolled) FROM temp_earliest_start_date;
EOF

    return cum_start_date.to_date rescue nil
  end

  def self.pregnant_and_breast_feeding_women(start_date, end_date)
    registered = [] ; patient_id_plus_date_enrolled = []; outcomes = []

    number_of_new_patients_registered = []; number_alive_and_on_arvs = []
    number_dead = []; number_defaulted = []; number_stopped_treatment = []
    number_transferred_out = []; number_unknown =[]
    data = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date t
      WHERE date_enrolled BETWEEN '#{start_date}' AND '#{end_date}'
      AND (gender = 'F' OR gender = 'Female') GROUP BY patient_id;
EOF

    (data || []).each do |patient|
      patient_id_plus_date_enrolled << [patient['patient_id'].to_i, patient['date_enrolled'].to_date]
    end

    yes_concept_id = ConceptName.find_by_name('Yes').concept_id

    concept_ids =[ConceptName.find_by_name('IS PATIENT PREGNANT?').concept_id,
                  ConceptName.find_by_name('PATIENT PREGNANT').concept_id,
                  ConceptName.find_by_name('PREGNANT AT INITIATION?').concept_id,
                  ConceptName.find_by_name('Breastfeeding').concept_id,
                  ConceptName.find_by_name('Is patient breast feeding?').concept_id,
                  ConceptName.find_by_name('Breast feeding?').concept_id]

    (patient_id_plus_date_enrolled || []).each do |patient_id, date_enrolled|
      result = ActiveRecord::Base.connection.select_value <<EOF
        SELECT * FROM obs
        WHERE obs_datetime BETWEEN '#{date_enrolled.strftime('%Y-%m-%d 00:00:00')}'
        AND '#{(date_enrolled.to_date + 30.days).strftime('%Y-%m-%d 23:59:59')}'
        AND person_id = #{patient_id}
        AND value_coded = #{yes_concept_id}
        AND concept_id IN (#{concept_ids.join(', ')})
        AND voided = 0 GROUP BY person_id;
EOF
      registered << {:patient_id => patient_id, :date_enrolled => date_enrolled } unless result.blank?
    end

    patient_ids = []
    (registered || []).each do |row|
      patient_ids << row[:patient_id].to_i
    end

    states = {
        'Quarter' => start_date.to_date.strftime("%Y"),
        'Number of new registered' => patient_ids.count,
        'Number Alive and on ART' => 0,
        'Number Dead' => 0, 'Number Defaulted' => 0 ,
        'Number Stopped Treatment' => 0, 'Number Transferred out' => 0,
        'Unknown' => 0}

    outcomes = self.get_survival_analysis_outcome(patient_ids, start_date, end_date)
    (outcomes || []).each do |row|
      #raise row['cum_outcome'].inspect
      if row['cum_outcome'] == 'On antiretrovirals'
        number_alive_and_on_arvs << row
        states['Number Alive and on ART']+=1
      elsif row['cum_outcome'] == 'Defaulted'
        number_defaulted << row
        states['Number Defaulted']+=1
      elsif row['cum_outcome'] == 'Patient transferred out'
        number_transferred_out << row
        states['Number Transferred out']+=1
      elsif row['cum_outcome'] == 'Patient died'
        number_dead << row
        states['Number Dead']+=1
      elsif row['cum_outcome'] == 'Treatment stopped'
        number_stopped_treatment << row
        states['Number Stopped Treatment']+=1
      else
      end
    end

    patient_outcomes = {
      "number_of_new_patients_registered" => data,
      "number_alive_and_on_arvs" => number_alive_and_on_arvs,
      "number_defaulted" => number_defaulted,
      "number_dead" => number_dead,
      "number_stopped_treatment" => number_stopped_treatment,
      "number_transferred_out" => number_transferred_out
    }

    return states, patient_outcomes
  end

  def self.children_analysis(start_date, end_date, min_age, max_age)
    registered = {}; data = []
    number_of_new_patients_registered = []; number_alive_and_on_arvs = []
    number_dead = []; number_defaulted = []; number_stopped_treatment = []
    number_transferred_out = []; number_unknown =[]

    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date
      WHERE earliest_start_date BETWEEN '#{start_date}' AND '#{end_date}'
      AND date_enrolled <= '#{end_date}'
      AND age_at_initiation BETWEEN "#{min_age}" AND "#{max_age}"
      GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      data << patient['patient_id'].to_i
    end

    data = [] if data.blank?

    registered = {
        'Quarter' => start_date.to_date.strftime("%Y"),
        'Number of new registered' => data.count,
        'Number Alive and on ART' => 0,
        'Number Dead' => 0, 'Number Defaulted' => 0 ,
        'Number Stopped Treatment' => 0, 'Number Transferred out' => 0,
        'Unknown' => 0}

    outcomes = self.get_survival_analysis_outcome(data, start_date, end_date)

    (outcomes || []).each do |row|
      #raise row['cum_outcome'].inspect
      if row['cum_outcome'] == 'On antiretrovirals'
        number_alive_and_on_arvs << row
        registered['Number Alive and on ART']+=1
      elsif row['cum_outcome'] == 'Defaulted'
        number_defaulted << row
        registered['Number Defaulted']+=1
      elsif row['cum_outcome'] == 'Patient transferred out'
        number_transferred_out << row
        registered['Number Transferred out']+=1
      elsif row['cum_outcome'] == 'Patient died'
        number_dead << row
        registered['Number Dead']+=1
      elsif row['cum_outcome'] == 'Treatment stopped'
        number_stopped_treatment << row
        registered['Number Stopped Treatment']+=1
      else
      end
    end

    states = {
      "number_of_new_patients_registered" => data,
      "number_alive_and_on_arvs" => number_alive_and_on_arvs,
      "number_defaulted" => number_defaulted,
      "number_dead" => number_dead,
      "number_stopped_treatment" => number_stopped_treatment,
      "number_transferred_out" => number_transferred_out
    }
    return registered, states
  end

  def self.general_analysis(start_date, end_date)
    registered = {}; data = []
    number_of_new_patients_registered = []; number_alive_and_on_arvs = []
    number_dead = []; number_defaulted = []; number_stopped_treatment = []
    number_transferred_out = []; number_unknown =[]

    total_registered = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_earliest_start_date
      WHERE earliest_start_date BETWEEN '#{start_date}' AND '#{end_date}'
      AND date_enrolled <= '#{end_date}' GROUP BY patient_id;
EOF

    (total_registered || []).each do |patient|
      data << patient['patient_id'].to_i
    end

    data = [] if data.blank?

    registered = {
      'Quarter' => start_date.to_date.strftime("%Y"),
        'Number of new registered' => data.count,
        'Number Alive and on ART' => 0,
        'Number Dead' => 0, 'Number Defaulted' => 0 ,
        'Number Stopped Treatment' => 0, 'Number Transferred out' => 0,
        'Unknown' => 0}

    outcomes = self.get_survival_analysis_outcome(data, start_date, end_date)
    (outcomes || []).each do |row|
      if row['cum_outcome'] == 'On antiretrovirals'
        number_alive_and_on_arvs << row
        registered['Number Alive and on ART']+=1
      elsif row['cum_outcome'] == 'Defaulted'
        number_defaulted << row
        registered['Number Defaulted']+=1
      elsif row['cum_outcome'] == 'Patient transferred out'
        number_transferred_out << row
        registered['Number Transferred out']+=1
      elsif row['cum_outcome'] == 'Patient died'
        number_dead << row
        registered['Number Dead']+=1
      elsif row['cum_outcome'] == 'Treatment stopped'
        number_stopped_treatment << row
        registered['Number Stopped Treatment']+=1
      else
      end
    end

    states = {
      "number_of_new_patients_registered" => data,
      "number_alive_and_on_arvs" => number_alive_and_on_arvs,
      "number_defaulted" => number_defaulted,
      "number_dead" => number_dead,
      "number_stopped_treatment" => number_stopped_treatment,
      "number_transferred_out" => number_transferred_out
    }
    return registered #, states
  end

  def self.get_survival_analysis_outcome(data, start_date, end_date)
    registered = []
    patient_ids = []

    data = [0] if data.blank?

    patient_ids = [0] if patient_ids.blank?

    total_alive_and_on_art = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM temp_patient_outcomes
      WHERE patient_id IN (#{data.join(', ')})
      GROUP BY patient_id;
EOF
    (total_alive_and_on_art || []).each do |patient|
      registered << patient
    end
    return registered
  end


end
