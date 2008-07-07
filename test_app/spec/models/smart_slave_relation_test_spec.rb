require File.dirname(__FILE__) + '/../spec_helper'
require File.dirname(__FILE__) + '/../../../init'
require File.dirname(__FILE__) + '/smart_slave_relation_test_helper'

describe SmartSlaveRelationTest do
  include SmartSlaveRelationTestHelper
  
  before(:each) do
    define_test_model :slave_db => :slave
    load_fixtures_data
  end
  
  it "should perform association finds in a smart way" do
    @rel = SmartSlaveRelationTest.find(1)
    @rel.smart_slave_test.name.should == "slave_test1"

    @rel = SmartSlaveRelationTest.find(5)
    @rel.smart_slave_test.name.should == "test5"
  end
end
