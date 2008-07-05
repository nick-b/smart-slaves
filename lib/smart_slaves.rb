module ActiveRecord; module Acts; end; end 

SMART_SLAVES_CLASSES = {}
SMART_SLAVES_CHECKPOINTS = {}

module SmartSlaves
  def self.included(base)
    base.extend(ClassMethods)
  end  

  module ClassMethods    
    def use_smart_slaves(params = {})
      params = {
        :db => :slave
      }.merge(params)
      
      check_params(params)
      
      self.slave_class = generate_slave_class(params[:db])
      self.checkpoint_value = nil
      
      self.extend(FinderClassOverrides)
    end

    alias_method :use_smart_slave, :use_smart_slaves

  protected
  
    def slave_class=(value)
      puts "slave_class=(#{value}) for #{self}"
      SMART_SLAVES_CLASSES[self] = value
    end

    def checkpoint_value=(value)
      puts "checkpoint_value=(#{value}) for #{self}"
      SMART_SLAVES_CHECKPOINTS[self] = value
    end
  
    def slave_class
      puts "slave_class for #{self} => #{SMART_SLAVES_CLASSES[self]}"
      SMART_SLAVES_CLASSES[self]
    end

    def checkpoint_value
      puts "checkpoint_value for #{self} => #{SMART_SLAVES_CHECKPOINTS[self]}"
      SMART_SLAVES_CHECKPOINTS[self]
    end

    def check_params(params)
      unless params[:db]
        raise "Invalid or no :db parameter passed!" 
      end
      
      unless ActiveRecord::Base.configurations[RAILS_ENV][params[:db].to_s]
        raise "No '#{params[:db]}' server defined in database.yml!"
      end
    end
    
    def slave_class_name(name)
      "SmartSlaveGenerated#{name.to_s.camelize}"
    end
    
    def generate_slave_class(name)
      ActiveRecord.module_eval %Q!
        class #{ slave_class_name(name) } < Base
          self.abstract_class = true
          establish_connection configurations[RAILS_ENV]['#{name}']
        end
      !
      "ActiveRecord::#{slave_class_name(name)}".constantize
    end
    
    module FinderClassOverrides
      def find(*opts)
        slave_options = opts.last.kind_of?(Hash) ? opts.pop : {}
        conn = choose_connection_by_ids(opts, slave_options)
        run_on_connection(conn) { super }
      end
      
    private
    
      def choose_connection_by_ids(ids, slave_options)
        puts "Choosing connection by IDS(#{ids.inspect})"
        return master_connection if slave_options[:on_master]
        
        ids = [ids].flatten
        ids.each do |rec_id|
          return master_connection if above_checkpoint?(rec_id)
        end
        return slave_connection
      end
      
      def master_connection
        connection
      end
      
      def slave_connection
        slave_class.connection
      end
      
      def above_checkpoint?(id)
        self.checkpoint_value ||= find_checkpoint_value
        puts "AboveCheckpoint? : #{id} > #{self.checkpoint_value}"
        id > checkpoint_value
      end

      def find_checkpoint_value
        run_on_connection(slave_connection) { maximum(primary_key) }
      end
      
      def run_on_connection(con)
        puts "Running a query on connection ##{con.object_id}"
        klass_conn = self.connection
        begin
          self.connection = con
          self.clear_active_connection_name
          yield
        ensure
          self.connection = klass_conn
          self.clear_active_connection_name
        end
      end
    end
  end
end

ActiveRecord::Base.send(:include, SmartSlaves)
