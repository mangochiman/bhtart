# Be sure to restart your server when you modify this file.

# Your secret key for verifying cookie session data integrity.
# If you change this key, all old sessions will become invalid!
# Make sure the secret is at least 30 characters and all random, 
# no regular words or you'll be exposed to dictionary attacks.
ActionController::Base.session = {
  :key         => '_conole_session',
  :secret      => '48ea4366866fd5ed6e1083e3c0123badddcaac4e22f9e2a20158596e7a17be872b2b5af7a0bfec562fabfbee5ccc935658605d39f7902217f95127a9b71ad3d7'
}

# Use the database for sessions instead of the cookie-based default,
# which shouldn't be used to store highly confidential information
# (create the session table with "rake db:sessions:create")
# ActionController::Base.session_store = :active_record_store
