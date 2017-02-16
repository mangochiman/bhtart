require 'fastercsv'

Source_db = YAML.load(File.open(File.join(RAILS_ROOT, "config/database.yml"), "r"))['production']["database"]

def start
 sample

end

def sample
  file = "./sample.csv"

  patient_details = ActiveRecord::Base.connection.select_all <<EOF
      SELECT esd.patient_id,
        esd.gender,
        esd.birthdate,
        DATE(esd.date_enrolled) AS date_of_registration,
        esd.earliest_start_date AS date_of_initiation,
        DATE(en.encounter_datetime) AS visit_date
      FROM earliest_start_date esd
        INNER JOIN encounter en ON en.patient_id = esd.patient_id AND en.voided AND en.encounter_type IN (6, 7, 9, 25, 51, 52, 53, 54, 68, 119)
      WHERE DATE(en.encounter_datetime) BETWEEN '2015-01-01' AND '2016-12-31 23:59:59'
      GROUP BY esd.patient_id, DATE(en.encounter_datetime);
EOF

  appointment_and_outcome_details = []
  (patient_details || []).each do |patient|
    appointment = ActiveRecord::Base.connection.select_one <<EOF
      SELECT person_id, concept_id, obs_datetime,  DATE(value_datetime) AS next_appointment_date FROM obs
        WHERE person_id = #{patient['patient_id']}
        AND DATE(obs_datetime) = '#{patient['visit_date']}'
        AND concept_id = 5096 AND voided = 0 LIMIT 1;
EOF

  outcome = ActiveRecord::Base.connection.select_one <<EOF
    SELECT patient_id, patient_outcome(#{patient['patient_id']}, '#{patient['visit_date']}') AS outcome
    FROM patient WHERE patient_id = #{patient['patient_id']} AND voided = 0;
EOF

    unless appointment.blank?
      appointment_date = appointment['next_appointment_date']
    else
      appointment_date = "0000-00-00"
    end

    puts "#{patient['patient_id']}, #{appointment_date}, #{outcome['outcome']}"
    appointment_and_outcome_details << [patient['patient_id'], patient['gender'], patient['birthdate'], patient['date_of_registration'], patient['date_of_initiation'], patient['visit_date'], appointment_date, outcome['outcome']]
  end

  FasterCSV.open( file, 'w' ) do |csv|
    csv << ["patient_id", "gender", "birthdate", "date_of_registration", "date_of_initiation", "visit_date", "next_appointment_date", "outcome"]
    appointment_and_outcome_details.each do |s|
      csv << [s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]
    end
  end
end
start
