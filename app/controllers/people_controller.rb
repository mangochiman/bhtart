class PeopleController < GenericPeopleController
  def create_person_from_anc
    ActiveRecord::Base.transaction do
      birth_day = params["person"]["birth_day"].to_i
      birth_month = params["person"]["birth_month"].to_i
      birthdate_estimated = 0
      if (birth_day == 0)
        birth_day = 1
        birthdate_estimated = 1
      end

      if (birth_month == 0)
        birth_month = 7
      end

      birthdate = Date.new(params["person"]["birth_year"].to_i,birth_month , birth_day)

      person = Person.create({
          :gender => params["person"]["gender"],
          :birthdate => birthdate,
          :birthdate_estimated => birthdate_estimated,
          :creator => 1
        })

      person.names.create({
          :given_name => params["person"]["names"]["given_name"],
          :family_name => params["person"]["names"]["family_name"],
          :family_name2 => params["person"]["names"]["family_name2"],
          :creator => 1
        })

      person.addresses.create({
          :address1 => params["person"]["addresses"]["address1"],
          :address2 => params["person"]["addresses"]["address2"],
          :city_village => params["person"]["addresses"]["city_village"],
          :county_district => params["person"]["addresses"]["county_district"],
          :creator => 1
        })

      person.person_attributes.create(
        :person_attribute_type_id => PersonAttributeType.find_by_name("Occupation").person_attribute_type_id,
        :value => params["person"]["attributes"]["occupation"],
        :creator => 1) unless params["person"]["attributes"]["occupation"].blank?

      person.person_attributes.create(
        :person_attribute_type_id => PersonAttributeType.find_by_name("Cell Phone Number").person_attribute_type_id,
        :value => params["person"]["attributes"]["cell_phone_number"],
        :creator => 1) unless params["person"]["attributes"]["cell_phone_number"].blank?

      person.person_attributes.create(
        :person_attribute_type_id => PersonAttributeType.find_by_name("Office Phone Number").person_attribute_type_id,
        :value => params["person"]["attributes"]["office_phone_number"],
        :creator => 1) unless params["person"]["attributes"]["office_phone_number"].blank?

      person.person_attributes.create(
        :person_attribute_type_id => PersonAttributeType.find_by_name("Home Phone Number").person_attribute_type_id,
        :value => params["person"]["attributes"]["home_phone_number"],
        :creator => 1) unless params["person"]["attributes"]["home_phone_number"].blank?

      patient = person.create_patient({:creator => 1})
      patient.patient_identifiers.create({
          :identifier => params["patient"]["identifiers"]["National id"],
          :identifier_type => PatientIdentifierType.find_by_name("NATIONAL ID").patient_identifier_type_id,
          :creator => 1
        })
    end
    render :text => "true" and return
  end

  def create_person_from_dmht
    User.current = User.first

    if create_from_dde_server
      national_id = DDEService.create_patient_from_dde(params, true)
      params["person"]["identifiers"] = {"national id" => national_id}
      PatientService.create_from_form(params["person"])
    else
      health_center_id = Location.current_health_center.location_id.to_s
      national_id_version = "1"
      national_id_prefix = "P#{national_id_version}#{health_center_id.rjust(3,"0")}"

      last_national_id = PatientIdentifier.find(:first,:order=>"identifier desc", :conditions => ["identifier_type = ? AND left(identifier,5)= ?", 
          PatientIdentifierType.find_by_name("NATIONAL ID").patient_identifier_type_id, national_id_prefix]
      )
      
      last_national_id_number = last_national_id.identifier rescue "0"

      next_number = (last_national_id_number[5..-2].to_i+1).to_s.rjust(7,"0")
      new_national_id_no_check_digit = "#{national_id_prefix}#{next_number}"
      check_digit = PatientIdentifier.calculate_checkdigit(new_national_id_no_check_digit[1..-1])
      national_id = "#{new_national_id_no_check_digit}#{check_digit}"

      params["person"]["identifiers"] = {"national id" => "#{national_id}"}
      PatientService.create_from_form(params["person"])
    end

    render :text => national_id and return
  end

end
 
