
Source_db = YAML.load(File.open(File.join(RAILS_ROOT, "config/database.yml"), "r"))['production']["database"]
CDCDataExtraction = 1

def start
  facility_name = GlobalProperty.find_by_sql("select property_value from global_property where property = 'current_health_center_name'").map(&:property_value).first

  start_date = "2016-10-01".to_date
  end_date = "2016-12-31".to_date
  puts "CDC TB-ART data extraction............................................................................................"

  puts "TB-HIV patients on ART................................................................................................"
  total_tb_hiv_on_art_total = tb_hiv_on_art_total(start_date, end_date, nil, nil)
  tb_hiv_on_art_total_less_15_female = tb_hiv_on_art_total(start_date, end_date, 0, 14, 'F')
  tb_hiv_on_art_total_less_15_male = tb_hiv_on_art_total(start_date, end_date, 0, 14, 'M')
  tb_hiv_on_art_total_more_15_female = tb_hiv_on_art_total(start_date, end_date,15, nil, 'F')
  tb_hiv_on_art_total_more_15_male = tb_hiv_on_art_total(start_date, end_date,15, nil, 'M')

  if CDCDataExtraction == 1
    $resultsOutput = File.open("./CDCDataExtraction_TBART" + "#{facility_name}" + ".txt", "w")
    $resultsOutput  << "TB-HIV patients on ART...........................................................\n"
    $resultsOutput  << "total_tb_hiv_on_art_total: #{total_tb_hiv_on_art_total}\n tb_hiv_on_art_total_less_15_female: #{tb_hiv_on_art_total_less_15_female}\n tb_hiv_on_art_total_less_15_male: #{tb_hiv_on_art_total_less_15_male}\n tb_hiv_on_art_total_more_15_female: #{tb_hiv_on_art_total_more_15_female}\n tb_hiv_on_art_total_more_15_male: #{tb_hiv_on_art_total_more_15_male}\n"
  end

  if CDCDataExtraction == 1
    $resultsOutput.close()
  end
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
      WHERE pp.program_id = 2
      AND ord.concept_id in (select distinct concept_id from concept_set where concept_set IN (1159))
      AND esd.gender = '#{gender}'
      AND DATE(ord.start_date) <= '#{end_date}'
      #{condition}
      GROUP BY esd.patient_id;
EOF
  else
    new_on_art = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM earliest_start_date esd
        INNER JOIN patient_program pp on pp.patient_id = esd.patient_id and pp.voided = 0
        INNER JOIN orders ord on ord.patient_id = pp.patient_id and ord.voided = 0
      WHERE pp.program_id = 2
      AND ord.concept_id in (select distinct concept_id from concept_set where concept_set IN (1159))
      AND DATE(ord.start_date) <= '#{end_date}'
      AND esd.gender IN ('F', 'M')
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
