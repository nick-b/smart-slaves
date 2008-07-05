class SmartSlaveTestOnSlave < ActiveRecord::Base
  establish_connection configuration[RAILS_ENV]['slave']
  set_table_name 'smart_slave_tests'
end
