module ActiveRecord; module Acts; end; end 

SMART_SLAVES_SLAVE_CLASSES = {}
SMART_SLAVES_MASTER_CLASSES = {}
SMART_SLAVES_DEFAULT_CLASSES = {}
SMART_SLAVES_CHECKPOINTS = {}

module SmartSlaves
  def self.included(base)
    base.extend(ClassMethods)
  end

  module ClassMethods    
    def use_smart_slaves(params = {})
      check_params(params)

      params[:default_finder] ||= :master
      
      self.master_class = (params[:master_db]) ? generate_ar_class(params[:master_db]) : ActiveRecord::Base
      self.slave_class = generate_ar_class(params[:slave_db])
      self.default_class = (params[:default_finder] == :master) ? self.master_class : self.slave_class

      self.checkpoint_value = nil
      
      self.extend(FinderClassOverrides)
    end

    alias_method :use_smart_slave, :use_smart_slaves

  protected
  
    def slave_class=(value)
      SMART_SLAVES_SLAVE_CLASSES[self] = value
    end

    def master_class=(value)
      SMART_SLAVES_MASTER_CLASSES[self] = value
    end

    def default_class=(value)
      SMART_SLAVES_DEFAULT_CLASSES[self] = value
    end

    def checkpoint_value=(value)
      puts "Setting checkpoint for #{self} to #{value}"
      SMART_SLAVES_CHECKPOINTS[self] = value
    end
  
    def slave_class
      SMART_SLAVES_SLAVE_CLASSES[self]
    end

    def master_class
      SMART_SLAVES_MASTER_CLASSES[self]
    end

    def default_class
      SMART_SLAVES_DEFAULT_CLASSES[self]
    end

    def checkpoint_value
      SMART_SLAVES_CHECKPOINTS[self]
    end

    def check_params(params)
      unless params[:slave_db]
        raise "Invalid or no :slave_db parameter passed!" 
      end
      
      unless ActiveRecord::Base.configurations[RAILS_ENV][params[:slave_db].to_s]
        raise "No '#{params[:slave_db]}' server defined in database.yml!"
      end
      
      if params[:master_db] && !ActiveRecord::Base.configurations[RAILS_ENV][params[:master_db].to_s]
        raise "No '#{params[:master_db]}' server defined in database.yml!"
      end
      
      if params[:default_finder] && ![:master, :slave].include?(params[:default_finder])
        raise ":default_finder should be either :master (default value) or :slave"
      end
    end
    
    def slave_class_name(name)
      "SmartSlaveGenerated#{name.to_s.camelize}"
    end
    
    def generate_ar_class(name)
      ActiveRecord.module_eval %Q!
        class #{ slave_class_name(name) } < Base
          self.abstract_class = true
          establish_connection configurations[RAILS_ENV]['#{name}']
        end
      !
      "ActiveRecord::#{slave_class_name(name)}".constantize
    end
    
    module FinderClassOverrides
      def find(*args)
        options = args.last.is_a?(Hash) ? args.last : {}
        options = cleanup_options(options)
        return run_on_default { super } if [:first, :last, :all].include?(args.first)
        
        ids = args.last.is_a?(Hash) ? args[0..-2] : args
        
        run_smart_by_ids(ids, options) { super }
      end

      def calculate(operation, column_name, options = {})
        return run_on_master { super } if options.delete(:on_master)
        return run_on_slave { super } if options.delete(:on_slave)
        run_on_default { super }
      end

      def master_connection
        master_class.connection
      end
      
      def slave_connection
        slave_class.connection
      end

      def default_connection
        default_class.connection
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
      
      def run_on_master
        run_on_connection(master_connection) { yield }
      end

      def run_on_slave
        run_on_connection(slave_connection) { yield }
      end

      def run_on_default
        run_on_connection(default_connection) { yield }
      end
      
    private

      def cleanup_options(options)
        slave_options = {}

        slave_options[:on_master] = options.delete(:on_master)
        slave_options[:on_slave] = options.delete(:on_slave)

        return slave_options
      end

      def run_smart_by_ids(ids, options = {})
        conn = choose_connection_by_ids(ids, options)
        run_on_connection(conn) { yield }
      end
    
      def choose_connection_by_ids(ids, slave_options)
        puts "Choosing connection by IDS(#{ids.inspect})"

        return master_connection if slave_options[:on_master]
        return slave_connection if slave_options[:on_slave]
        
        ids = [ids].flatten
        puts "...based on PK ids: #{ids.inspect}"
        ids.each do |rec_id|
          return default_connection if above_checkpoint?(rec_id)
        end
        
        puts "Slave connection selected"
        return slave_connection
      end
      
      def above_checkpoint?(id)
        self.checkpoint_value ||= find_checkpoint_value
        puts "AboveCheckpoint? : #{id} > #{self.checkpoint_value}"
        id > checkpoint_value
      end

      def find_checkpoint_value
        maximum(primary_key, :on_slave => true)
      end
      
    end
  end
end

ActiveRecord::Base.send(:include, SmartSlaves)
