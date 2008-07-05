class SmartSlaveTestOnSlave < ActiveRecord::Base
  establish_connection :test_slave
  set_table_name 'smart_slave_tests'
end
