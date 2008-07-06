module SmartSlavesHelper
  def define_test_model(opts)
    SmartSlaveTest.class_eval do
      use_smart_slaves(opts)
    end
  end
  
  def create_fixture_record(klass, params)
    rec = klass.new(params)
    rec.id = params[:id]
    rec.save
  end
  
  def load_fixtures_data
    SmartSlaveTestOnSlave.connection.execute("DELETE FROM smart_slave_tests")
    create_fixture_record(SmartSlaveTestOnSlave, :name => 'slave_test1', :id => 1)
    create_fixture_record(SmartSlaveTestOnSlave, :name => 'slave_test2', :id => 2)
    create_fixture_record(SmartSlaveTestOnSlave, :name => 'slave_test3', :id => 3)

    SmartSlaveTest.connection.execute("DELETE FROM smart_slave_tests")
    create_fixture_record(SmartSlaveTest, :name => 'test1', :id => 1)
    create_fixture_record(SmartSlaveTest, :name => 'test2', :id => 2)
    create_fixture_record(SmartSlaveTest, :name => 'test3', :id => 3)
    create_fixture_record(SmartSlaveTest, :name => 'test4', :id => 4)
    create_fixture_record(SmartSlaveTest, :name => 'test5', :id => 5)
    create_fixture_record(SmartSlaveTest, :name => 'test6', :id => 6)
  end
end
