=begin
    Author: mangochiman
    Purpose:
          1. Identify HIV infected women
          2. Delete all data in the system that are not related to HIV infected women.
            2.1 If the HIV infected woman has other programs that re not HIV related, delete them
    Start Date: 19/June/2015
    End Date: ??/June/2015
=end

def hiv_infected_women
  hiv_infected_women_ids = ActiveRecord::Base.connection.select_all(
    "SELECT esd.patient_id FROM earliest_start_date esd INNER JOIN person p ON
      esd.patient_id = p.person_id AND p.gender = 'F'").collect{|i|i["patient_id"]}
  hiv_infected_women_ids = ['0'] if hiv_infected_women_ids.blank?

  patients_to_be_deleted = Patient.find(:all, :conditions => ["patient_id NOT IN (?)",
      hiv_infected_women_ids])
  patients_to_be_deleted.each do |patient|
    ActiveRecord::Base.transaction do
      ActiveRecord::Base.connection.execute("DELETE FROM person_address WHERE person_id=#{patient.id}")
      ActiveRecord::Base.connection.execute("DELETE FROM person_relationship WHERE person_a=#{patient.id}")
      ActiveRecord::Base.connection.execute("DELETE FROM person_attributes WHERE person_id=#{patient.id}")
      ActiveRecord::Base.connection.execute("DELETE FROM obs WHERE person_id=#{patient.id}")
      ActiveRecord::Base.connection.execute("DELETE FROM person WHERE person_id=#{patient.id}")
      ActiveRecord::Base.connection.execute("
          DELETE drug_o FROM drug_order drug_o INNER JOIN order o on drug_o.order_id = o.order_id
          INNER JOIN encounter e ON e.encounter_id = o.encounter_id WHERE e.patient_id = #{patient.id}
        ")
      ActiveRecord::Base.connection.execute("
          DELETE o FROM order o INNER JOIN encounter e ON o.encounter_id = e.encounter_id
          WHERE e.patient_id = #{patient.id}
        ")

      ActiveRecord::Base.connection.execute("
          DELETE FROM order WHERE patient_id = #{patient.id}
        ")
      ActiveRecord::Base.connection.execute("DELETE FROM encounter WHERE patient_id=#{patient.id}")
      ActiveRecord::Base.connection.execute("
          DELETE ps FROM patient_state ps INNER JOIN patient_program pp ON
          ps.patient_program_id = pp.patient_program_id WHERE pp.patient_id = #{patient.id}
        ")
      ActiveRecord::Base.connection.execute("
          DELETE FROM patient_program WHERE patient_id = #{patient.id}
        ")
      ActiveRecord::Base.connection.execute("
          DELETE FROM patient_identifier WHERE patient_id = #{patient.id}
        ")

      ActiveRecord::Base.connection.execute("
          DELETE FROM relationship WHERE person_a= #{patient.id}
        ")
      
    end

    #Checking and Deleting other non HIV related programs for HIV infected women
    hiv_program_id = Program.find_by_name('HIV PROGRAM').id
    hiv_infected_women_ids.each do |id|
     ActiveRecord::Base.connection.execute("
          DELETE ps FROM patient_state ps INNER JOIN patient_program pp ON
          ps.patient_program_id = pp.patient_program_id WHERE pp.patient_id = #{id} AND
          pp.program_id != #{hiv_program_id}
        ")
     ActiveRecord::Base.connection.execute("
          DELETE FROM patient_program WHERE patient_id = #{id} AND program_id!= #{hiv_program_id}
        ")
    end
  end
  
end

hiv_infected_women