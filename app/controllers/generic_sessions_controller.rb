class GenericSessionsController < ApplicationController
	skip_before_filter :authenticate_user!, :except => [:location, :update]
	skip_before_filter :location_required

	def new
	end


	def create
		user = User.authenticate(params[:login], params[:password])
		sign_in(:user, user) if user
		authenticate_user! if user

		session[:return_uri] = nil
		session[:datetime] = nil

		if user_signed_in?
			current_user.reset_authentication_token
			#my_token = current_user.authentication_token
			#User.find_for_authentication_token()
			#self.current_user = user      
			redirect_to '/clinic'
		else
			note_failed_signin
			@login = params[:login]
			render :action => 'new'
		end
	end

	# Form for entering the location information
	def location
		@login_wards = (CoreService.get_global_property_value('facility.login_wards')).split(',') rescue []
		if (CoreService.get_global_property_value('select_login_location').to_s == "true" rescue false)
			render :template => 'sessions/select_location'
		end
    
    @activate_drug_management = CoreService.get_global_property_value('activate.drug.management').to_s == "true" rescue false
=begin
    if (@activate_drug_management)
      @stock = {}
      drug_names = GenericDrugController.new.preformat_regimen
      drug_names.each do |drug_name|
        drug = Drug.find_by_name(drug_name)
        drug_pack_size = Pharmacy.pack_size(drug.id)
        current_stock = (Pharmacy.latest_drug_stock(drug.id)/drug_pack_size).to_i #In tins
        next unless (current_stock.to_i == 0)
        consumption_rate = Pharmacy.average_drug_consumption(drug.id)
        stock_out_days = ((current_stock * drug_pack_size)/consumption_rate).to_i rescue 0 #To avoid division by zero error when consumption_rate is zero
        estimated_stock_out_date = (Date.today + stock_out_days).strftime('%d-%b-%Y')
        estimated_stock_out_date = "(N/A)" if (consumption_rate.to_i <= 0)
        estimated_stock_out_date = "Stocked out" if (current_stock <= 0) #We don't want to estimate the stock out date if there is no stock available

        @stock[drug.id] = {}
        @stock[drug.id]["drug_name"] = drug.name
        @stock[drug.id]["current_stock"] = current_stock
        @stock[drug.id]["consumption_rate"] = consumption_rate.to_f.round(1)
        @stock[drug.id]["estimated_stock_out_date"] = estimated_stock_out_date
        @stock[drug.id]["drug_pack_size"] = drug_pack_size
      end
      @stock = @stock.sort_by{|drug_id, values|values["drug_name"]}
    end
=end
	end

  def stock_levels_graph
    @current_heath_center_name = Location.current_health_center.name rescue '?'
    @list = {}
    drug_names = GenericDrugController.new.preformat_regimen
    drug_cms_hash = {}
    DrugCms.all.each do |drug_cms|
      drug_cms_hash[drug_cms.drug_inventory_id] = drug_cms.name
    end
    drug_names.each do |drug_name|

      drug = Drug.find_by_name(drug_name)
      next if drug_cms_hash[drug.id].blank?
      drug_pack_size = Pharmacy.pack_size(drug.id)
      current_stock = (Pharmacy.latest_drug_stock(drug.id)/drug_pack_size).to_i #In tins
      consumption_rate = Pharmacy.average_drug_consumption(drug.id)

      stock_level = current_stock # stock level comes in pills/day here we convert it to tins/month
      disp_rate = (consumption_rate.to_f * 0.5) #rate is an avg of pills dispensed per day. here we convert it to tins per month

      consumption_rate = (consumption_rate.to_f * 0.5)
      expected = stock_level.round
      month_of_stock = (expected/consumption_rate) rescue 0

      stocked_out = (disp_rate.to_i != 0 && month_of_stock.to_f.round(3) == 0.00)

      active = (disp_rate.to_i == 0 && stock_level.to_i != 0)? false : true
      drug_cms_name = drug_cms_hash[drug.id]
      @list[drug_cms_name] = {
        "month_of_stock" => month_of_stock,
        "stock_level" => stock_level,
        "consumption_rate" => disp_rate.round(3),
        "stocked_out" => stocked_out,
        "active" => active
      }

    end
  end

	# Update the session with the location information
	def update    
		# First try by id, then by name
		location = Location.find(params[:location]) rescue nil
		location ||= Location.find_by_name(params[:location]) rescue nil

		valid_location = (generic_locations.include?(location.name)) rescue false

		unless location and valid_location
			flash[:error] = "Invalid workstation location"

			@login_wards = (CoreService.get_global_property_value('facility.login_wards')).split(',') rescue []
			if (CoreService.get_global_property_value('select_login_location').to_s == "true" rescue false)
				render :template => 'sessions/select_location'
			else
				render :action => 'location'
			end
			return    
		end

		self.current_location = location
		role = current_user.user_roles.map{|r|r.role}

		if use_user_selected_activities and not location.name.match(/Outpatient/i) and not role.include?("Pharmacist")
			redirect_to "/user/programs/#{current_user.id}"
		else
			redirect_to '/clinic'
		end
	end

	def destroy
		sign_out(current_user) if !current_user.blank?
		self.current_location = nil
		flash[:notice] = "You have been logged out."
		redirect_back_or_default('/')
	end

	protected
  # Track failed login attempts
  def note_failed_signin
    flash[:error] = "Invalid user name or password"
    logger.warn "Failed login for '#{params[:login]}' from #{request.remote_ip} at #{Time.now.utc}"
  end
end
