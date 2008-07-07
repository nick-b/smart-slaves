module SmartSlaveRelationTestHelper
  def load_fixtures_data
    SmartSlaveRelationTest.connection.execute("DELETE FROM smart_slave_relation_tests")
    create_fixture_record(SmartSlaveRelationTest, :name => 'relation1', :id => 1, :smart_slave_test_id => 1)
    create_fixture_record(SmartSlaveRelationTest, :name => 'relation2', :id => 2, :smart_slave_test_id => 3)
    create_fixture_record(SmartSlaveRelationTest, :name => 'relation3', :id => 3, :smart_slave_test_id => 3)
    create_fixture_record(SmartSlaveRelationTest, :name => 'relation4', :id => 4, :smart_slave_test_id => 4)
    create_fixture_record(SmartSlaveRelationTest, :name => 'relation5', :id => 5, :smart_slave_test_id => 5)
    create_fixture_record(SmartSlaveRelationTest, :name => 'relation6', :id => 6, :smart_slave_test_id => 6)
  end
end
