module DataGeneratorService
  require 'csv'
  
  env = RAILS_ENV
  openmrs_con = YAML.load(File.open(File.join(RAILS_ROOT, "config/database.yml"), "r"))[env]
  bart_uri = "http://#{openmrs_con['host']}:3002"

  def self.list_of_patients_without_any_encounters
    Thread.new{app.post("/sessions/create",{'username' => 'admini', 'password' => 'test'})}
    Thread.new{app.post("/sessions/update",{'location' => 721})}

    patients = ActiveRecord::Base.connection.select_all <<EOF
      SELECT * FROM patient p 
      INNER JOIN person_name n ON n.person_id = p.patient_id AND n.voided = 0
      INNER JOIN person ON person.person_id = p.patient_id
      WHERE p.voided = 0 
      AND patient_id NOT IN(SELECT e.patient_id FROM encounter e WHERE e.voided = 0)
      GROUP BY p.patient_id ORDER BY n.date_created DESC
EOF

    return patients
  end

end
