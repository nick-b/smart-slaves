require File.dirname(__FILE__) + '/../spec_helper'
require File.dirname(__FILE__) + '/../../../init'
require File.dirname(__FILE__) + '/smart_slave_test_helper'

describe SmartSlaves do
  include SmartSlaveTestHelper
  
  it "should raise an exception if no :slave_db parameter passed" do
    lambda {
      define_test_model
    }.should raise_error
  end
  
  it "should raise and exception if invalid :slave_db parameter passed" do
    lambda {
      define_test_model :slave_db => :crap
    }.should raise_error
  end

  it "should raise and exception if invalid :default_finder parameter passed" do
    lambda {
      define_test_model :default_finder => :crap, :slave_db => :slave
    }.should raise_error
  end

  it "should not raise and exception if all params are valid" do
    lambda {
      define_test_model :slave_db => :slave
    }.should_not raise_error
  end

  it "should run find on the slave or the master server ignoring :default_finder option" do
    load_fixtures_data
    
    define_test_model :slave_db => :slave, :default_finder => :master
    SmartSlaveTest.find(1).name.should == "slave_test1"
    SmartSlaveTest.find(:first, :conditions => { :id => 1 }).name.should == "slave_test1"

    define_test_model :slave_db => :slave, :default_finder => :slave
    SmartSlaveTest.find(5).name.should == "test5"
    SmartSlaveTest.find(:first, :conditions => { :id => 5 }).name.should == "test5"
  end
end

describe SmartSlaves, "with use_smart_slaves:" do
  include SmartSlaveTestHelper
  
  before(:each) do
    define_test_model :slave_db => :slave
    load_fixtures_data
  end

  describe "find method override" do
    it "should retrieve and store in the cache our checkpoint id" do
      SmartSlaveTest.send(:checkpoint_value=, nil)
      SmartSlaveTest.find(5).name.should == "test5"
      SmartSlaveTest.send(:checkpoint_value).should be(3)
    end
  
    it "should do finds on an appropriate servers (based on id)" do
      SmartSlaveTest.find(2).name.should == "slave_test2"
      SmartSlaveTest.find(3).name.should == "slave_test3"
      SmartSlaveTest.find(4).name.should == "test4"
      SmartSlaveTest.find(5).name.should == "test5"
    end

    it "should do finds on an appropriate servers (based on many ids)" do
      SmartSlaveTest.find(1, 2).collect(&:name).should == ["slave_test1", "slave_test2" ]
      SmartSlaveTest.find(2, 3).collect(&:name).should == ["slave_test2", "slave_test3" ]
      SmartSlaveTest.find(3, 4).collect(&:name).should == ["test3", "test4" ]
      SmartSlaveTest.find(4, 5).collect(&:name).should == ["test4", "test5" ]
    end
  
    it "should support conditioned find calls" do
      SmartSlaveTest.find(:first, :conditions => { :id => 2 }).name.should == "slave_test2"
      SmartSlaveTest.find(:first, :conditions => { :id => 5 }).name.should == "test5"
      SmartSlaveTest.find(:all, :conditions => { :id => [1, 2] }).collect(&:name).should == ["slave_test1", "slave_test2" ]
      SmartSlaveTest.find(:all, :conditions => { :id => [3, 4] }).collect(&:name).should == ["test3", "test4" ]
      SmartSlaveTest.find(:all, :conditions => { :id => 1..3 }).collect(&:name).should == ["slave_test1", "slave_test2", "slave_test3"]
      SmartSlaveTest.find(:all, :conditions => { :id => 2..4 }).collect(&:name).should == ["test2", "test3", "test4" ]
    end
  
    it "should run find on the master when :on_master => true is in the options" do
      SmartSlaveTest.find(1, :on_master => true).name.should == "test1"
      SmartSlaveTest.find(:first, :conditions => { :id => 1 }, :on_master => true).name.should == "test1"
    end

    it "should run find on the slave when :on_slave => true is in the options" do
      lambda { SmartSlaveTest.find(5, :on_slave => true) }.should raise_error(ActiveRecord::RecordNotFound)
      SmartSlaveTest.find(:first, :conditions => { :id => 5 }, :on_slave => true).should be_nil
    end
  end
  
  describe "calculate method override" do
    it "should honor :on_master and :on_slave options" do
      SmartSlaveTest.calculate(:max, :id, :on_master => true).should == 6
      SmartSlaveTest.calculate(:max, :id, :on_slave => true).should == 3
    end
    
    it "should honor default_finder option" do
      define_test_model :slave_db => :slave, :default_finder => :master
      SmartSlaveTest.calculate(:max, :id).should == 6

      define_test_model :slave_db => :slave, :default_finder => :slave
      SmartSlaveTest.calculate(:max, :id).should == 3
    end
    
    describe "should work for high-level calculations too:" do
      it "maximum" do
        SmartSlaveTest.maximum(:id, :on_master => true).should == 6
        SmartSlaveTest.maximum(:id, :on_slave => true).should == 3
      end

      it "minimum" do
        SmartSlaveTest.minimum(:name, :on_master => true).should == "test1"
        SmartSlaveTest.minimum(:name, :on_slave => true).should == "slave_test1"
      end
      
      it "average" do
        SmartSlaveTest.average(:id, :on_master => true).should == 3.5
        SmartSlaveTest.average(:id, :on_slave => true).should == 2
      end
    end
  end
  
  describe "find_by_sql and construct_finder_sql methods overrides" do
    it "should honor :on_master option" do
      sql = "SELECT * FROM smart_slave_tests WHERE name='test1'"
      SmartSlaveTest.find_by_sql(sql, :on_master => true).first.id.should == 1
    end

    it "should honor :on_slave option" do
      sql = "SELECT * FROM smart_slave_tests WHERE name='slave_test1'"
      SmartSlaveTest.find_by_sql(sql, :on_slave => true).first.id.should == 1
    end
    
    describe "should support dynamic methods:" do
      it "find_by_field_name" do
        SmartSlaveTest.find_by_name("test1", :on_master => true).id.should == 1
        SmartSlaveTest.find_by_name("slave_test1", :on_slave => true).id.should == 1
      end

      it "find_all_by_field_name" do
        SmartSlaveTest.find_all_by_name("test1", :on_master => true).first.id.should == 1
        SmartSlaveTest.find_all_by_name("slave_test1", :on_slave => true).first.id.should == 1
      end
    end
  end
  
  describe "run_on_* methods: " do
    it "run_on_master should force all queries to be performed on the master" do
      SmartSlaveTest.run_on_master do
        sql = "SELECT * FROM smart_slave_tests WHERE id = 1"
        SmartSlaveTest.find_by_sql(sql).first.name.should == "test1"
      end
    end

    it "run_on_slave should force all queries to be performed on the slave" do
      SmartSlaveTest.run_on_slave do
        sql = "SELECT * FROM smart_slave_tests WHERE id = 1"
        SmartSlaveTest.find_by_sql(sql).first.name.should == "slave_test1"
      end
    end
    
    it "should support multi-level run_on_* blocks" do
      sql = "SELECT * FROM smart_slave_tests WHERE id = 1"
      SmartSlaveTest.run_on_master do
        SmartSlaveTest.find_by_sql(sql).first.name.should == "test1"
        SmartSlaveTest.run_on_slave do
          SmartSlaveTest.find_by_sql(sql).first.name.should == "slave_test1"
        end
        SmartSlaveTest.find_by_sql(sql).first.name.should == "test1"
      end
    end
    
  end
end
