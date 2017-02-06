
Source_db = YAML.load(File.open(File.join(RAILS_ROOT, "config/database.yml"), "r"))['production']["database"]
CDCDataExtraction = 1

def start
  facility_name = GlobalProperty.find_by_sql("select property_value from global_property where property = 'current_health_center_name'").map(&:property_value).first

  start_date = "2016-10-01".to_date
  end_date = "2016-12-31".to_date
  puts "CDC TB-ART data extraction............................................................................................"

  puts "TB-HIV patients on ART................................................................................................"
#=begin
  $total_tb_hiv_on_art_total = tb_hiv_on_art_total(start_date, end_date, nil, nil)
  tb_hiv_on_art_total_less_15_female = tb_hiv_on_art_total(start_date, end_date, 0, 14, 'F')
  tb_hiv_on_art_total_less_15_male = tb_hiv_on_art_total(start_date, end_date, 0, 14, 'M')
  tb_hiv_on_art_total_more_15_female = tb_hiv_on_art_total(start_date, end_date,15, nil, 'F')
  tb_hiv_on_art_total_more_15_male = tb_hiv_on_art_total(start_date, end_date,15, nil, 'M')
#=end
  puts "TB-ART patients (HIV)................................................................................................."
  total_tb_art_hiv_total = tb_art_hiv_total($total_tb_hiv_on_art_total, start_date, end_date, nil, nil)
  tb_art_hiv_total_less_15_female = tb_art_hiv_total($total_tb_hiv_on_art_total, start_date, end_date, 0, 14, 'F')
  tb_art_hiv_total_less_15_male = tb_art_hiv_total($total_tb_hiv_on_art_total, start_date, end_date, 0, 14, 'M')
  tb_art_hiv_total_more_15_female = tb_art_hiv_total($total_tb_hiv_on_art_total, start_date, end_date,15, nil, 'F')
  tb_art_hiv_total_more_15_male = tb_art_hiv_total($total_tb_hiv_on_art_total, start_date, end_date,15, nil, 'M')

  if CDCDataExtraction == 1
    $resultsOutput = File.open("./CDCDataExtraction_TBART" + "#{facility_name}" + ".txt", "w")
    $resultsOutput  << "TB-HIV patients on ART...........................................................\n"
    $resultsOutput  << "total_tb_hiv_on_art_total: #{total_tb_hiv_on_art_total}\n tb_hiv_on_art_total_less_15_female: #{tb_hiv_on_art_total_less_15_female}\n tb_hiv_on_art_total_less_15_male: #{tb_hiv_on_art_total_less_15_male}\n tb_hiv_on_art_total_more_15_female: #{tb_hiv_on_art_total_more_15_female}\n tb_hiv_on_art_total_more_15_male: #{tb_hiv_on_art_total_more_15_male}\n"
    $resultsOutput  << "TB-HIV patients...........................................................\n"
    $resultsOutput  << "tb_art_hiv_total_total: #{tb_art_hiv_total}\n tb_art_hiv_total_less_15_female: #{tb_art_hiv_total_less_15_female}\n tb_art_hiv_total_less_15_male: #{tb_art_hiv_total_less_15_male}\n tb_art_hiv_total_more_15_female: #{tb_art_hiv_total_more_15_female}\n tb_art_hiv_total_more_15_male: #{tb_art_hiv_total_more_15_male}\n"
  end

  if CDCDataExtraction == 1
    $resultsOutput.close()
  end
end

def self.tb_art_hiv_total(patients_on_arvs, start_date, end_date, min_age = nil, max_age = nil, gender = [])
 patient_ids = []

 patients_on_arvs = [0] if patients_on_arvs.blank?
=begin
 (patients_on_arvs || []).each do |row|
   puts '#{row}'
   patient_ids << row['patient_id'
 end
=end
  if (max_age.blank? && min_age.blank?)
    condition = ""
  elsif (max_age.blank?)
    condition = "HAVING age_at_initiation >= #{min_age}"
  else
    condition = "HAVING age_at_initiation  BETWEEN #{min_age} and #{max_age}"
  end
#raise gender.inspect
 #1159 = tb drugs concept_set
  if !gender.blank?
    new_on_art = ActiveRecord::Base.connection.select_all <<EOF
      SELECT pp.patient_id, (select timestampdiff(year, p.birthdate, min(pp.date_enrolled))) AS age_at_initiation FROM patient_program pp
        INNER JOIN orders ord on ord.patient_id = pp.patient_id and ord.voided = 0
        INNER JOIN obs o on o.person_id = ord.patient_id and o.voided = 0
        INNER JOIN person p on p.person_id = pp.patient_id
      WHERE pp.program_id = 2
      AND ord.concept_id in (select distinct concept_id from concept_set where concept_set IN (1159))
      AND pp.patient_id not in (#{patients_on_arvs})
      AND o.concept_id = 3753 and o.value_coded = 703
      AND p.gender = '#{gender}'
      AND DATE(pp.date_enrolled) <= '#{end_date}'
      GROUP BY pp.patient_id
      #{condition};
EOF
  else
    new_on_art = ActiveRecord::Base.connection.select_all <<EOF
      SELECT pp.patient_id, (select timestampdiff(year, p.birthdate, min(pp.date_enrolled))) AS age_at_initiation FROM patient_program pp
        INNER JOIN orders ord on ord.patient_id = pp.patient_id and ord.voided = 0
        INNER JOIN obs o on o.person_id = ord.patient_id and o.voided = 0
        INNER JOIN person p on p.person_id = pp.patient_id
      WHERE pp.program_id = 2
      AND ord.concept_id in (select distinct concept_id from concept_set where concept_set IN (1159))
      AND pp.patient_id not in (#{patients_on_arvs})
      AND o.concept_id = 3753 and o.value_coded = 703
      AND p.gender = '#{gender}'
      AND DATE(pp.date_enrolled) <= '#{end_date}'
      GROUP BY pp.patient_id
      #{condition};
EOF
  end

  if new_on_art.blank?
    result = 0
  else
    result = new_on_art.count
  end
  return  result
end

def self.tb_hiv_on_art_total(start_date, end_date, min_age = nil, max_age = nil, gender = [])
  if (max_age.blank? && min_age.blank?)
    condition = ""
  elsif (max_age.blank?)
    condition = "AND age_at_initiation >= #{min_age}"
  else
    condition = "AND age_at_initiation  BETWEEN #{min_age} and #{max_age}"
  end
#raise gender.inspect
 #1159 = tb drugs concept_set
  if !gender.blank?
    new_on_art = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM earliest_start_date esd
        INNER JOIN patient_program pp on pp.patient_id = esd.patient_id and pp.voided = 0
        INNER JOIN orders ord on ord.patient_id = pp.patient_id and ord.voided = 0
        INNER JOIN person p on p.person_id = esd.patient_id
      WHERE pp.program_id = 2
      AND ord.concept_id in (select distinct concept_id from concept_set where concept_set IN (1159))
      AND p.gender = '#{gender}'
      AND DATE(ord.start_date) <= '#{end_date}'
      #{condition}
      GROUP BY esd.patient_id;
EOF
  else
    new_on_art = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM earliest_start_date esd
        INNER JOIN patient_program pp on pp.patient_id = esd.patient_id and pp.voided = 0
        INNER JOIN orders ord on ord.patient_id = pp.patient_id and ord.voided = 0
        INNER JOIN person p on p.person_id = esd.patient_id
      WHERE pp.program_id = 2
      AND ord.concept_id in (select distinct concept_id from concept_set where concept_set IN (1159))
      AND DATE(ord.start_date) <= '#{end_date}'
      AND p.gender IN ('F', 'M')
      #{condition}
      GROUP BY esd.patient_id;
EOF
  end

  if new_on_art.blank?
    result = 0
  else
    result = new_on_art.count
  end
  return  result
end

start
