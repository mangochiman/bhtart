module MedicationService
  require 'csv'

	def self.arv(drug)
		arv_drugs.map(&:concept_id).include?(drug.concept_id) rescue false
	end

	def self.arv_drugs
		arv_concept       = ConceptName.find_by_name("ANTIRETROVIRAL DRUGS").concept_id
		arv_drug_concepts = ConceptSet.all(:conditions => ['concept_set = ?', arv_concept])
		arv_drug_concepts
	end

	def self.tb_medication(drug)
		tb_drugs.map(&:concept_id).include?(drug.concept_id)
	end

	def self.tb_drugs
		tb_medication_concept       = ConceptName.find_by_name("Tuberculosis treatment drugs").concept_id
		tb_medication_drug_concepts = ConceptSet.all(:conditions => ['concept_set = ?', tb_medication_concept])
		tb_medication_drug_concepts
	end
	
	def self.diabetes_medication(drug)
		diabetes_drugs.map(&:concept_id).include?(drug.concept_id)
	end	
	
	def self.diabetes_drugs
		diabetes_medication_concept       = ConceptName.find_by_name("DIABETES MEDICATION").concept_id
		diabetes_medication_drug_concepts = ConceptSet.all(:conditions => ['concept_set = ?', diabetes_medication_concept])
		diabetes_medication_drug_concepts
	end

  # Generate a given list of Regimen+s for the given +Patient+ <tt>weight</tt>
  # into select options. 
	def self.regimen_options(weight, program)
		regimens = Regimen.find(	:all,
									:order => 'regimen_index',
									:conditions => ['? >= min_weight AND ? < max_weight AND program_id = ?', weight, weight, program.program_id])

		options = regimens.map { |r|
			concept_name = (r.concept.concept_names.typed("SHORT").first ||	r.concept.concept_names.typed("FULLY_SPECIFIED").first).name
			if r.regimen_index.blank?
				["#{concept_name}", r.concept_id, r.regimen_index.to_i]
			else
				["#{r.regimen_index} - #{concept_name}", r.concept_id, r.regimen_index.to_i]
			end
		}.sort_by{| r | r[2]}.uniq

		return options
	end

  def self.all_regimen_options(program)
		regimens = Regimen.find(	:all,
									:order => 'regimen_index',
									:conditions => ['program_id = ?', program.program_id])

		options = regimens.map { |r|
			concept_name = (r.concept.concept_names.typed("SHORT").first ||	r.concept.concept_names.typed("FULLY_SPECIFIED").first).name
			if r.regimen_index.blank?
				["#{concept_name}", r.concept_id, r.regimen_index.to_i]
			else
				["#{r.regimen_index} - #{concept_name}", r.concept_id, r.regimen_index.to_i]
			end
		}.sort_by{| r | r[2]}.uniq

		return options
	end
	
  def self.current_orders(patient)
    encounter = current_treatment_encounter(patient)
    orders = encounter.orders.active
    orders
  end
  
  def self.current_treatment_encounter(patient)
    type = EncounterType.find_by_name("TREATMENT")
    encounter = patient.encounters.current.find_by_encounter_type(type.id)
    encounter ||= patient.encounters.create(:encounter_type => type.id)
  end

  def self.generic
    #tag_id = ConceptNameTag.find_by_tag("preferred_qech_aetc_opd").concept_name_tag_id
 
 		medication_tag = CoreService.get_global_property_value("application_generic_medication")
 			   
    all_drugs = Drug.all.collect {|drug|
      # [Concept.find(drug.concept_id).name.name, drug.concept_id] rescue nil

      [(drug.concept.fullname rescue drug.concept.shortname rescue ' '), drug.concept_id]
      #[ConceptName.find(:last, :conditions => ["concept_id = ? AND voided = 0 AND concept_name_id IN (?)", 
      #      drug.concept_id, ConceptNameTagMap.find(:all, :conditions => ["concept_name_tag_id = ?", tag_id]).collect{|c| 
      #        c.concept_name_id}]).name, drug.concept_id] rescue nil
    
    }.compact.uniq  rescue []
    
    if !medication_tag.blank?
    	application_drugs = concept_set(medication_tag)
    else
    	application_drugs = all_drugs
    end
    return_drugs = all_drugs - (all_drugs - application_drugs) 
  end

  def self.frequencies
    ConceptName.find_by_sql("SELECT name FROM concept_name WHERE concept_id IN \
                        (SELECT answer_concept FROM concept_answer c WHERE \
                        concept_id = (SELECT concept_id FROM concept_name \
                        WHERE name = 'DRUG FREQUENCY CODED')) AND concept_name_id \
                        IN (SELECT concept_name_id FROM concept_name_tag_map \
                        WHERE concept_name_tag_id = (SELECT concept_name_tag_id \
                        FROM concept_name_tag WHERE tag = 'preferred_dmht'))").collect {|freq|
                            freq.name rescue nil
                        }.compact rescue []
  end
  
	def self.fully_specified_frequencies
		concept_id = ConceptName.find_by_name('DRUG FREQUENCY CODED').concept_id
		set = ConceptSet.find_all_by_concept_set(concept_id, :order => 'sort_weight')
		frequencies = []
		options = set.each{ | item | 
			next if item.concept.blank?
			frequencies << [item.concept.shortname, item.concept.fullname + "(" + item.concept.shortname + ")"]
		}
		frequencies
	end
  
	def self.dosages(generic_drug_concept_id)    
		Drug.find(:all, :conditions => ["concept_id = ?", generic_drug_concept_id]).collect {|d|
			["#{d.name.upcase rescue ""}", "#{d.dose_strength.to_f rescue 1}", "#{d.units.upcase rescue ""}"]
		}.uniq.compact rescue []
	end
	
  def self.concept_set(concept_name)
    concept_id = ConceptName.find(:first, :conditions =>["name = ?", concept_name]).concept_id
    set = ConceptSet.find_all_by_concept_set(concept_id, :order => 'sort_weight')
    options = set.map{|item|next if item.concept.blank? ; [item.concept.fullname, item.concept.concept_id] }
    return options
  end

  def self.get_arv_regimen(patient_id, dispension_date)
    possible_combinations = {}
    csv_url =  RAILS_ROOT + "/doc/regimens_possible_combinations.csv"

    CSV.foreach("#{csv_url}") do |row|
      next if row[0].strip.match(/regimen/i)
      regimen = row[0].strip.upcase
      if possible_combinations[regimen].blank?
        possible_combinations[regimen] = []
      end
      possible_combinations[regimen] << row[1].strip
    end
   
    amount_dispensed_concept = ConceptName.find_by_name('Amount dispensed').concept
    dispensed_drugs = []
    dispensed_arvs_ids = Observation.find_by_sql("
      SELECT drug_inventory_id FROM obs
      INNER JOIN drug_order d ON d.order_id = obs.order_id
      AND obs.voided = 0 AND obs.concept_id = #{amount_dispensed_concept.id}
      WHERE obs.person_id = #{patient_id} AND obs.obs_datetime 
      BETWEEN '#{dispension_date.to_date.strftime('%Y-%m-%d 00:00:00')}' 
      AND '#{dispension_date.to_date.strftime('%Y-%m-%d 23:59:59')}' 
      AND d.drug_inventory_id IN(SELECT drug_id FROM arv_drug);")  
    
    return if dispensed_arvs_ids.blank?
 
    dispensed_regimen = 'Unknown'
    dispensed_arvs_ids = dispensed_arvs_ids.map{|i|i.drug_inventory_id}.uniq

    (possible_combinations || {}).each do |regimen, drug_ids|
      (drug_ids).each do |ids|
        arv_ids = ids.split(';')
        if arv_ids.length == dispensed_arvs_ids.length
          if (arv_ids - dispensed_arvs_ids) == []
            dispensed_regimen = regimen
          end
        end
      end 
    end
    return dispensed_regimen
  end

  def self.latest_arv_dispensed_date(patient_id)
    amount_dispensed_concept = ConceptName.find_by_name('Amount dispensed').concept
    
    obs = Observation.find_by_sql("
      SELECT MAX(obs_datetime) obs_datetime FROM obs
      INNER JOIN drug_order d ON d.order_id = obs.order_id
      AND obs.voided = 0 AND obs.concept_id = #{amount_dispensed_concept.id}
      WHERE obs.person_id = #{patient_id} 
      AND d.drug_inventory_id IN(SELECT drug_id FROM arv_drug);")  
     
    return if obs.blank?
    return obs.first.obs_datetime
  end

  def self.moh_arv_regimen_options(current_weight)
    regimen_categories = {}
    regimens = MohRegimen.find(:all, :joins => "INNER JOIN moh_regimen_lookup l 
      ON l.regimen_id = moh_regimens.regimen_id", :select => "moh_regimens.*, l.*")

    regimen_ingrients = {}
    ingrients = MohRegimenIngredient.all

    (ingrients || []).each do |r|
      regimen_ingrients[r.regimen_id] = [] if regimen_ingrients[r.regimen_id].blank?
      if current_weight >= r.min_weight and current_weight <= r.max_weight
        regimen_ingrients[r.regimen_id] << r.drug_inventory_id 
      end
    end

    moh_regimens = {}
    (MohRegimenLookup.all || []).each do |lookup|
      moh_regimens[lookup.regimen_name] = [] if moh_regimens[lookup.regimen_name].blank?
      moh_regimens[lookup.regimen_name] << lookup.drug_inventory_id
    end

    recommended_regimens = []

    (moh_regimens || {}).each do |regimen_name, drug_inventory_ids|
      (regimen_ingrients || {}).each do |regimen_id, drug_ids|
        if (drug_ids - drug_inventory_ids) == [] and (drug_inventory_ids.count == drug_ids.count)
          regimen_index = MohRegimen.find(regimen_id).regimen_index
          recommended_regimens << "Regimen #{regimen_index}"
        end
      end
    end

    
    return recommended_regimens.sort_by{|x| x.gsub('Regimen ','').to_i}
  end

  def self.regimen_medications(regimen_index, current_weight)
    regimen_index = regimen_index.to_s.gsub('Regimen ','').to_i 
    regimen_id = MohRegimen.find(:first, :conditions =>['regimen_index = ?', regimen_index]).regimen_id
    regimen_medications = Drug.find(:all,:joins => "INNER JOIN moh_regimen_ingredient i 
      ON i.drug_inventory_id = drug.drug_id AND i.regimen_id = #{regimen_id}
      INNER JOIN moh_regimen_doses d ON d.dose_id = i.dose_id",
      :conditions => "#{current_weight} >= min_weight AND #{current_weight} <= max_weight",
      :select => "drug.*, i.*, d.*").map do |medication|
        {
          :drug_name => medication.name,
          :am => medication.am,
          :pm => medication.pm,
          :units => medication.units,
          :regimen_index => regimen_index
        }
      end

    return regimen_medications    
  end


end
