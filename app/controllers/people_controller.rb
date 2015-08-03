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

end
 
