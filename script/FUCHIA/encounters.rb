require 'fastercsv'
User.current = User.find_by_username('admin')
    
Hiv_reception_encounter = EncounterType.find_by_name("Hiv reception")
Vitals = EncounterType.find_by_name("Vitals")
HIV_staging = EncounterType.find_by_name("HIV staging")
Treatment = EncounterType.find_by_name("TREATMENT")
Dispension = EncounterType.find_by_name("DISPENSING")
Appointment = EncounterType.find_by_name("APPOINTMENT")
HIV_clinic_consultation = EncounterType.find_by_name("HIV CLINIC CONSULTATION")
Parent_path = '/home/pachawo/Documents/msf/'

@@staging_conditions = []
@@patient_visits = {}
@@drug_map = {}
@@drug_follow_up = {}

def start
  FasterCSV.foreach("#{Parent_path}/tb_follow_up.csv", :headers => true, :quote_char => '"', :col_sep => ',', :row_sep => :auto) do |row|
    patient_id = row[3].to_i
    date_created = get_proper_date(row[9]).to_date rescue nil
    next_visit = get_proper_date(row[10]).to_date rescue nil
    ref_id = row[0].to_i
    next if date_created.blank? || patient_id.blank?
    start_date = date_created.strftime("%Y-%m-%d 00:00:00")
    end_date = date_created.strftime("%Y-%m-%d 23:59:59")

    encounter = Encounter.find(:all, :conditions => ["patient_id = ? and encounter_datetime 
     between ? and ? and encounter_type = ?",patient_id, start_date, end_date, 
     Hiv_reception_encounter.id])

    if encounter.blank?
      self.create_hiv_reception(patient_id, date_created)
    else
      puts "Patient has encounters recorded!!"
    end



   ############################### creating vitals ###########################################
   weight = row[28].to_f rescue nil
   height = row[29].to_f rescue nil
   vitals_encounter = nil

   unless weight.blank? 
     vitals_encounter = self.create_encounter(patient_id, Vitals.id, date_created)
     self.create_observation_value_numeric(vitals_encounter,"Weight (kg)", weight)
   end
   
   unless height.blank?
     vitals_encounter = self.create_encounter(patient_id, Vitals.id, date_created) if vitals_encounter.blank?
     self.create_observation_value_numeric(vitals_encounter, "Height (cm)", height)
   end
   ###########################################################################################


  diagnostic = row[30] rescue nil
  diagnostic_2 = row[31] rescue nil
  cd4_count = row[23].to_i
  date_cd4_count = get_proper_date(row[14]).to_date unless row[14].blank?

  ############################ hiv staging and clinical consultation #########################
  s_c = @@patient_visits[patient_id][date_created] rescue []

  hiv_staging_encounter = Encounter.find(:last, :conditions => ["patient_id = ? and encounter_datetime <= ? 
    and encounter_type = ?",patient_id, end_date, HIV_staging.id])


  if hiv_staging_encounter.blank?
   hiv_staging_encounter = self.create_encounter(patient_id, HIV_staging.id, date_created) if hiv_staging_encounter.blank?
   (s_c || []).each do |cond|
     self.create_observation_value_coded(hiv_staging_encounter, "WHO Stage defining conditions not explicitly asked adult", cond)
     puts "#{hiv_staging_encounter.patient_id}:::::::::: #{cond}"
    end 
  end 

=begin
  if diagnostic_2.match(/preg/i) || diagnostic.match(/preg/i)
    self.create_observation_value_coded(hiv_staging_encounter, 'Pregnant', 'Yes')
  end rescue nil 
=end

  if !cd4_count.blank? && !date_cd4_count.blank?
    self.create_observation_value_numeric(hiv_staging_encounter, 'CD4 count', cd4_count)
    self.create_observation_value_datetime(hiv_staging_encounter, 'Date of CD4 count', date_cd4_count)
  end rescue nil

  hiv_clinic_registration_encounter = self.create_encounter(patient_id, HIV_clinic_consultation.id, date_created)

  if diagnostic_2.match(/preg/i) || diagnostic.match(/preg/i)
    self.create_observation_value_coded(hiv_clinic_registration_encounter, 'Pregnant', 'Yes')
  end rescue nil 

  if diagnostic_2.match(/NEUROPATH/i) || diagnostic.match(/NEUROPATH/i)
    self.create_observation_value_coded(hiv_clinic_registration_encounter,'MALAWI ART SIDE EFFECTS', 'Peripheral neuropathy')
  end rescue nil

  if diagnostic_2.match(/anaemia/i) || diagnostic.match(/anaemia/i)
    self.create_observation_value_coded(hiv_clinic_registration_encounter, 'MALAWI ART SIDE EFFECTS', 'Anemia')
  end rescue nil

  if diagnostic_2.match(/jaundice/i) || diagnostic.match(/jaundice/i)
    self.create_observation_value_coded(hiv_clinic_registration_encounter,'MALAWI ART SIDE EFFECTS', 'Jaundice')
  end rescue nil

  if diagnostic_2.match(/psychosis/i) || diagnostic.match(/psychosis/i)
    self.create_observation_value_coded(hiv_clinic_registration_encounter, 'MALAWI ART SIDE EFFECTS','Psychosis')
  end rescue nil

          # >>>>>>>>>>>>>>>>>>> Associated symptoms <<<<<<<<<<<<<<<<<<<<< #
  
  if diagnostic_2.match(/cough/i) || diagnostic.match(/cough/i)
    self.create_observation_value_coded(hiv_clinic_registration_encounter, 'TB symptoms', 'Cough')
  end rescue nil

  if diagnostic_2.match(/fever/i) || diagnostic.match(/fever/i)
    self.create_observation_value_coded(hiv_clinic_registration_encounter, 'TB symptoms',  'Fever')
  end rescue nil

  if diagnostic_2.match(/malnutrition/i) || diagnostic.match(/malnutrition/i) || diagnostic_2(/failure to thrive/i) || diagnostic(/failure to thrive/i) || diagnostic_2(/weight loss/i) || diagnostic(/weight loss/i)
    self.create_observation_value_coded(hiv_clinic_registration_encounter, 'TB symptoms',  'Weight loss / Failure to thrive / malnutrition')
  end rescue nil

  ##########################################################################################

  medication_on_this_visit = @@drug_follow_up[ref_id][date_created] rescue nil

  ############################ give drugs ##################################################
  (medication_on_this_visit || []).each do |medication|
    next if next_visit.blank?
    duration = (next_visit - date_created).to_i
    #puts "............. #{medication}"
  end
  
  unless medication_on_this_visit.blank?
    #prescription_enc = self.create_encounter(patient_id, Treatment.id, date_created) 
    #dispension_enc = self.create_encounter(patient_id, Dispension.id, date_created) 
  end

  ##########################################################################################

  ################################ set appointment #########################################
  if !next_visit.blank?
    appointment_encounter = self.create_encounter(patient_id, Appointment.id, date_created)
    #raise appointment_encounter.inspect
    self.create_observation_value_datetime(appointment_encounter, "Appointment date", next_visit)
    puts "Setting appointment for...... #{patient_id}"
  end
  ##########################################################################################

  end
end

#function that loads csv file data into a hash
def get_references(parent_path, ref_id)
  FasterCSV.foreach("#{parent_path}/TbReference.csv", :headers => true, :quote_char => '"', :col_sep =>',', :row_sep =>:auto) do |row|
    if row[0].to_i.equal?(ref_id.to_i)
     return row[6]
    end
  end
end

#function that patient_drug csv to a hash
def get_patient_drug(parent_path, patient_id)
  count = 0
  drug_ref = Array.new
  FasterCSV.foreach ("#{parent_path}/TbPatientDrug.csv", :headers =>true, :quote_char => '"', :col_sep => ',', :row_sep =>:auto) do |row|
    if row[3].to_i.equal?(patient_id.to_i)
      drug_ref.insert(count,row[4])
      count = count + 1
    end
  end
  return drug_ref
end

def self.create_hiv_reception(patient_id, date_created)
  #TODO create hiv reception
  puts "HIV reception for: #{patient_id}"
  encounter = self.create_encounter(patient_id, Hiv_reception_encounter.id, date_created)
  if !encounter.blank?
   #TODO create observations

    self.create_observation_value_coded(encounter, "Guardian present", "No") #guardian obs
    
    self.create_observation_value_coded(encounter, "Patient present", "Yes") #guardian obs
  end
end

def self.create_encounter(patient_id, encounter_type_id, date_created)
  if !patient_id.blank?
    encounter = Encounter.new
    encounter.encounter_type = encounter_type_id
    encounter.patient_id = patient_id
    encounter.encounter_datetime = date_created.strftime("%Y-%m-%d 00:00:00")
    encounter.save
    return encounter
  end
end

def self.create_observation_value_numeric(encounter, concept_name, value)
    observation =  Observation.new
    observation.person_id = encounter.patient_id
    observation.encounter_id = encounter.id
    observation.concept_id = ConceptName.find_by_name(concept_name).concept_id
    observation.value_numeric = value
    observation.obs_datetime = encounter.encounter_datetime
    observation.save
    puts ">>> Creating vitals #{concept_name}"
end

def self.create_observation_value_coded(encounter, concept_name, value_coded_concept_name)
    observation =  Observation.new
    observation.person_id = encounter.patient_id
    observation.encounter_id = encounter.id
    observation.concept_id = ConceptName.find_by_name(concept_name).concept_id
    observation.value_coded = ConceptName.find_by_name(value_coded_concept_name).concept_id
    observation.obs_datetime = encounter.encounter_datetime
    observation.save
end

def self.create_observation_value_datetime(encounter, concept_name, date)
    observation =  Observation.new
    observation.person_id = encounter.patient_id
    observation.encounter_id = encounter.id
    observation.concept_id = ConceptName.find_by_name(concept_name).concept_id
    observation.value_datetime = date.strftime("%Y-%m-%d %H:%M:%S")
    observation.obs_datetime = encounter.encounter_datetime
    observation.save
end

def get_proper_date (unfomatted_date)
  if !unfomatted_date.blank?
    unfomatted_date = unfomatted_date.split("/")
    year_of_birth = unfomatted_date[2].split(" ")
    year_of_birth = year_of_birth[0]
    current_year = Date.today.year.to_s

    if year_of_birth.to_i > current_year[-2..-1].to_i
      year = "19#{year_of_birth}"
    else
      year = "20#{year_of_birth}"
    end

    fomatted_date = "#{year}-#{unfomatted_date[0]}-#{unfomatted_date[1]}"
  else
    return nil
  end
end

def setup_staging_conditions

  @@conditions_map = {"Weight loss >10%" => 'Severe weight loss >10% and/or BMI <18.5kg/m^2, unexplained',
    	"Fever, unexplained" => 'Fever, persistent unexplained, intermittent or constant, >1 month',
    	"In bed > 50 of normal daytime due to sickness" => 'Unknown',
   	  "Kaposi sarcoma" => 'Kaposi sarcoma',
	    "Herpes zoster" => 'Herpes zoster',
	    "Wasting syndrome by HIV/stunting/severe malnutrition" => 'HIV wasting syndrome (severe weight loss + persistent fever or severe weight loss + chronic diarrhoea)',
	    "Minor mucocutaneous manifestations" => 'Minor mucocutaneous manifestations',
	    "Weight loss <10%" => 'Moderate weight loss less than or equal to 10 percent, unexplained',
	    "Pulmonary TB" => 'Pulmonary TB (current)',
	    "URTI" => 'URTI',
	    "Diarrhoea unexplained" => 'Diarrhoea, chronic (>1 month) unexplained',
	    "Oral candidiasis" => 'Oral candidiasis',
	    "Bacterial pneumonia, severe" => 'Bacterial pneumonia, severe recurrent',
    	"Asymptomatic" => 'Asymptomatic',
    	"Extrapulmonary and disseminated TB" => 'Extrapulmonary tuberculosis (EPTB)',
    	"Bacterial infections, severe" => 'Bacterial infections, severe recurrent  (empyema, pyomyositis, meningitis, bone/joint infections but EXCLUDING pneumonia)',
    	"Candidiasis oesophagus/trachea/bronchi /lungs" => 'Candidiasis of oseophagus',
    	"Cryptococcosis extrapulmonary" => 'Cryptococcosis, extrapulmonary',
    	"Vulvovaginal candidiasis > 1 month..." => 'Candidiasis vulvovaginal',
    	"In bed < 50 of normal daytime due to sickness" => 'Unknown',
    	"Oral hairy leukoplakia" => 'Oral hairy leukoplakia',
    	"Herpes simplex infection" => 'Herpes simplex',
    	"Encephalopathy by HIV" => 'Encephalopathy HIV',
    	"Lymphoma cerebral or B non Hodgkin" => 'Lymphoma (cerebral or B-cell non hodgkin)',
    	"Cytomegalovirus infection" => 'Cytomegalovirus infection',
    	"Mycosis disseminated" => 'Disseminated mycosis (coccidiomycosis or histoplasmosis)',
    	"Toxoplasmosis of the brain" => 'Toxoplasmosis of the brain',
    	"Septicaemia recurrent" => 'Unknown',
    	"HIV-associated nephropathy" => 'HIV associated nephropathy',
    	"Pneumocystis pneumonia" => 'Pneumocystis pneumonia',
    	"Angular cheilitis" => 'Angular cheilitis',
    	"Isosporiasis" => 'Isosporiasis', 
    	"Seborrheic dermatitis" => 'Seborrhoeic dermatitis',
    	"Wart infection" => 'Wart virus infection',
    	"Papular pruritic eruption" => 'Papular pruritic eruptions'
  }

  conditions = []

  FasterCSV.foreach("#{Parent_path}/TbFollowUpDiagnosis.csv", :headers => true, :quote_char => '"', :col_sep => ',', :row_sep => :auto) do |row|
    conditions << row[4].to_i
    conditions = conditions.uniq
  end

  FasterCSV.foreach("#{Parent_path}/TbPatientDiagnosis.csv", :headers => true, :quote_char => '"', :col_sep => ',', :row_sep => :auto) do |row|
    patient_id = row[3].to_i
    date_created = get_proper_date(row[1]).to_date rescue nil
    next if date_created.blank? 
    
    if @@patient_visits[patient_id].blank?
      @@patient_visits[patient_id] = {}
      @@patient_visits[patient_id][date_created] = []
   else
      if @@patient_visits[patient_id][date_created].blank?
        @@patient_visits[patient_id][date_created] = []
      end
   end
   @@patient_visits[patient_id][date_created] << @@conditions_map[get_references(Parent_path,row[4])]
   @@patient_visits[patient_id][date_created] = @@patient_visits[patient_id][date_created].uniq
  end
end

def drug_mapping
  @@drug_map = {
    "Cotrimoxazole prophylaxis" => 'Cotrimoxazole',
    "FDC3 (AZT-3TC-NVP)" => 'AZT+3TC+NVP',
    "Efavirenz 600" => 'Efavirenz',
    "Isoniazide prophylaxis" => '',
    "FDC11 (TDF-3TC-EFV)" => 'TDF/3TC/EFV (300/300/600mg tablet)',
    "FDC1 (D4T30-3TC-NVP)" => 'D4T30+3TC+NVP',
    "Lamivudine" => 'Lamivudine',
    "Stavudine (dosage unspecified)" => 'Stavudine',
    "Nevirapine" => 'Nevirapine',
    "FDC2 pediatric (AZT-3TC-NVPp)" => '',
    "FDC10 (TDF-3TC)" => 'TDF/3TC',
    "Atazanavir/Ritonavir" => 'Atazanavir Ritonavir',
    "FDC2 (D4T40-3TC-NVP)" => 'D4T40+3TC+NVP',
    "FDC5 (D4T30-3TC)" => '',
    "Dapsone prophylaxis" => 'Dapsone',
    "Kaletra (Lopinavir/Ritonavir) pediatric" => 'Kaletra',
    "FDC5 pediatric (ABC-3TCp)" => 'BC+3TC',
    "FDC7 (AZT-3TC)" => '',
    "Fluconazole secondary prophylaxis" => 'Fluconazole',
    "FDC4 pediatric (AZT-3TCp)" => '',
    "Stavudine 30" => 'Stavudine',
    "FDC1 pediatric (D4T30-3TC-NVPp)" => 'D4T30+3TC+NVP',
    "Kaletra (Lopinavir/Ritonavir)" => '',
    "Efavirenz pediatric" =>'EFV (Efavirenz 50mg tablet)',
    "Nevirapine pediatric" => 'NVP (Nevirapine 50 mg tablet)',
    "Lamivudine pediatric" => '3TC (Lamivudine 150mg tablet)',
    "Abacavir pediatric" => 'ABC (Abacavir 300mg tablet)',
    "Ritonavir" => 'Ritonavir 100mg',
    "Darunavir" => 'Darunavir 600mg',
    "Raltegravir" => 'RAL (Raltegravir 400mg)',
    "Abacavir" => 'ABC (Abacavir 300mg tablet)',
    "Fluconazole primary prophylaxis" => 'FCZ (Fluconazole 150mg tablet)',
    "Tenofovir" => 'TDF (Tenofavir 300 mg tablet)',
    "FDC3 pediatric (D4T30-3TCp)" =>  'd4T/3TC (Stavudine Lamivudine 30/150 tablet)',
    "Zidovudine" => 'AZT (Zidovudine 300mg tablet)',
    "FDC12 (TDF-3TC-NVP)" => 'TDF/3TC (Tenofavir and Lamivudine 300/300mg tablet',
    "Nelfinavir" => 'NFV(Nelfinavir)',
    "Didanosine 250" => 'Didanosine',
    "Zidovudine (Mother to child)" => '',
    "Nevirapine (Mother to child)" => '',
    "Didanosine 400" => 'Didanosine',
    "Other ARV 2" => 'ARV other2 recommendation',
    "Zidovudine pediatric" => 'Zidovudine',
    "FDC6 (D4T40-3TC)" => '',
    "Other ARV 1" => 'ARV other1 recommendation',
    "Stavudine pediatric" => 'Stavudine',
    "Stavudine 40" => 'Stavudine',
    "Nelfinavir pediatric" => 'Nelfinavir',
    "Efavirenz 800" => 'Efavirenz',
    "Atazanavir" => 'Atazanavir',
    "NVP Single Dose + (AZT-3TC) regimen (Mother to child)" => ''
  }

  drugs = []

  FasterCSV.foreach("#{Parent_path}/TbFollowUpDrug.csv", :headers => true, :quote_char => '"', :col_sep => ',', :row_sep => :auto) do |row|
   drugs << row[4].to_i
   drugs = drugs.uniq
  end

  FasterCSV.foreach("#{Parent_path}/TbFollowUpDrug.csv", :headers => true, :quote_char => '"', :col_sep => ',', :row_sep => :auto) do |row|
    follow_up_ref = row[3].to_i
    date_created = get_proper_date(row[1]).to_date rescue nil
    next if date_created.blank? 

    if @@drug_follow_up[follow_up_ref].blank?
      @@drug_follow_up[follow_up_ref] = {}
      @@drug_follow_up[follow_up_ref][date_created] = []
    else
      if @@drug_follow_up[follow_up_ref][date_created].blank?
        @@drug_follow_up[follow_up_ref][date_created] = []
      end
    end
    @@drug_follow_up[follow_up_ref][date_created] << @@drug_map[get_references(Parent_path,row[4])]
    @@drug_follow_up[follow_up_ref][date_created] = @@drug_follow_up[follow_up_ref][date_created].uniq
  end

end

#drug_mapping
#setup_staging_conditions
start