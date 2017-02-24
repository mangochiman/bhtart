
Source_db = YAML.load(File.open(File.join(RAILS_ROOT, "config/database.yml"), "r"))['production']["database"]
CDCDataExtraction = 1

def start
  facility_name = GlobalProperty.find_by_sql("select property_value from global_property where property = 'current_health_center_name'").map(&:property_value).first

  start_date = "2016-10-01".to_date
  end_date = "2016-12-31".to_date
  puts "CDC TB-ART data extraction............................................................................................"

  puts "TB-HIV patients on ART................................................................................................"
  total_tb_hiv_on_art_total, tb_hiv_on_art_total_less_15_female, tb_hiv_on_art_total_less_15_male, tb_hiv_on_art_total_more_15_female, tb_hiv_on_art_total_more_15_male = tb_hiv_on_art_total(start_date, end_date, nil, nil)

  puts "TB-ART patients (HIV)................................................................................................."
  total_tb_art_hiv_total, tb_art_hiv_total_less_15_female, tb_art_hiv_total_less_15_male, tb_art_hiv_total_more_15_female, tb_art_hiv_total_more_15_male= tb_art_hiv_total(start_date, end_date, nil, nil)

  if CDCDataExtraction == 1
    $resultsOutput = File.open("./CDCDataExtraction_TBART" + "#{facility_name}" + ".txt", "w")
    $resultsOutput  << "TB-HIV patients on ART...........................................................\n"
    $resultsOutput  << "total_tb_hiv_on_art_total: #{total_tb_hiv_on_art_total}\n tb_hiv_on_art_total_less_15_female: #{tb_hiv_on_art_total_less_15_female}\n tb_hiv_on_art_total_less_15_male: #{tb_hiv_on_art_total_less_15_male}\n tb_hiv_on_art_total_more_15_female: #{tb_hiv_on_art_total_more_15_female}\n tb_hiv_on_art_total_more_15_male: #{tb_hiv_on_art_total_more_15_male}\n"
    $resultsOutput  << "TB-HIV patients...........................................................\n"
    $resultsOutput  << "tb_art_hiv_total_total: #{total_tb_art_hiv_total}\n tb_art_hiv_total_less_15_female: #{tb_art_hiv_total_less_15_female}\n tb_art_hiv_total_less_15_male: #{tb_art_hiv_total_less_15_male}\n tb_art_hiv_total_more_15_female: #{tb_art_hiv_total_more_15_female}\n tb_art_hiv_total_more_15_male: #{tb_art_hiv_total_more_15_male}\n"
  end

  if CDCDataExtraction == 1
    $resultsOutput.close()
  end
end

def self.tb_art_hiv_total(start_date, end_date, min_age = nil, max_age = nil, gender = [])
 patient_ids = []
 new_on_art = ActiveRecord::Base.connection.select_all <<EOF
   SELECT * FROM earliest_start_date esd
     INNER JOIN patient_program pp on pp.patient_id = esd.patient_id and pp.voided = 0
     INNER JOIN orders ord on ord.patient_id = pp.patient_id and ord.voided = 0
     INNER JOIN person p on p.person_id = esd.patient_id
   WHERE pp.program_id = 2
   AND ord.concept_id in (select distinct concept_id from concept_set where concept_set IN (1159))
   AND DATE(ord.start_date) <= '#{end_date}'
   GROUP BY esd.patient_id;
EOF

    (new_on_art || []).each do |patient|
      patient_ids << patient['patient_id'].to_i
    end

 #1159 = tb drugs concept_set
  new_on_art_with_tb = ActiveRecord::Base.connection.select_all <<EOF
      SELECT pp.patient_id, (select timestampdiff(year, p.birthdate, min(pp.date_enrolled))) AS age_at_initiation, p.gender FROM patient_program pp
        INNER JOIN orders ord on ord.patient_id = pp.patient_id and ord.voided = 0
        INNER JOIN obs o on o.person_id = ord.patient_id and o.voided = 0
        INNER JOIN person p on p.person_id = pp.patient_id
      WHERE pp.program_id = 2
      AND ord.concept_id in (select distinct concept_id from concept_set where concept_set IN (1159))
      AND pp.patient_id not in (#{patient_ids.join(',')})
      AND o.concept_id = 3753 and o.value_coded = 703
      AND DATE(pp.date_enrolled) <= '#{end_date}'
      GROUP BY pp.patient_id
EOF

  total_tb_art_hiv_total = []
  tb_art_hiv_total_less_15_female = []
  tb_art_hiv_total_less_15_male = []
  tb_art_hiv_total_more_15_female = []
  tb_art_hiv_total_more_15_male = []

  (new_on_art_with_tb || []).each do |patient|
    if patient['age_at_initiation'].to_f < 15
      if patient['gender'] == "F"
        tb_art_hiv_total_less_15_female << patient
      else
        tb_art_hiv_total_less_15_male << patient
      end
    elsif patient['age_at_initiation'].to_f >= 15
      if patient['gender'] == "F"
        tb_art_hiv_total_more_15_female << patient
      else
        tb_art_hiv_total_more_15_male << patient
      end
    end
  end
  total_tb_art_hiv_total = new_on_art_with_tb

  return [
    total_tb_art_hiv_total.count,
    tb_art_hiv_total_less_15_female.count,
    tb_art_hiv_total_less_15_male.count,
    tb_art_hiv_total_more_15_female.count,
    tb_art_hiv_total_more_15_male.count
  ]
end

def self.tb_hiv_on_art_total(start_date, end_date, min_age = nil, max_age = nil, gender = [])
    new_on_art = ActiveRecord::Base.connection.select_all <<EOF
      SELECT esd.* FROM earliest_start_date esd
        INNER JOIN patient_program pp on pp.patient_id = esd.patient_id and pp.voided = 0
        INNER JOIN orders ord on ord.patient_id = pp.patient_id and ord.voided = 0
        INNER JOIN person p on p.person_id = esd.patient_id
      WHERE pp.program_id = 2
      AND ord.concept_id in (select distinct concept_id from concept_set where concept_set IN (1159))
      AND DATE(ord.start_date) <= '#{end_date}'
      GROUP BY esd.patient_id;
EOF

  total_tb_hiv_on_art_total = []
  tb_hiv_on_art_total_less_15_female = []
  tb_hiv_on_art_total_less_15_male = []
  tb_hiv_on_art_total_more_15_female = []
  tb_hiv_on_art_total_more_15_male = []

  (new_on_art || []).each do |patient|
    if patient['age_at_initiation'].to_f < 15
      if patient['gender'] == "M"
        tb_hiv_on_art_total_less_15_male << patient
      else
        tb_hiv_on_art_total_less_15_female << patient
      end
    end
    if patient['age_at_initiation'].to_f >= 15
      if patient['gender'] == "M"
        tb_hiv_on_art_total_more_15_male << patient
      else
        tb_hiv_on_art_total_more_15_female << patient
      end
    end
  end

  total_tb_hiv_on_art_total = new_on_art
  return [total_tb_hiv_on_art_total.count, tb_hiv_on_art_total_less_15_female.count,
            tb_hiv_on_art_total_less_15_male.count,
            tb_hiv_on_art_total_more_15_female.count,
            tb_hiv_on_art_total_more_15_male.count]

end

start
