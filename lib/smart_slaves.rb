module ActiveRecord; module Acts; end; end 

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
      @@slave_class = generate_slave_class(params[:db])
      @@checkpoint_value = nil
      self.extend(FinderClassOverrides)
    end

    alias_method :use_smart_slave, :use_smart_slaves

  private
  
    def check_params(params)
      unless params[:db]
        raise "Invalid or no :db parameter passed!" 
      end
      
      unless configurations[RAILS_ENV][params[:db]]
        raise "No #{params[:db]} server defined in database.yml!"
      end
    end
    
    def slave_class_name(name)
      "SmartSlaveGenerated#{name.camelize}"
    end
    
    def generate_slave_class(name)
      ActiveRecord.module_eval %Q!
        class #{ slave_class_name(name) } < Base
          self.abstract_class = true
          establish_connection configurations[RAILS_ENV]['#{name}']
        end
      !
    end
    
    module FinderClassOverrides
      def find(*opts, slave_options = {})
        conn = choose_connection_by_ids(opts, slave_options)
        run_on_connection(conn) { super }
      end
      
    private
    
      def choose_connection_by_ids(ids, slave_options)
        return master_connection if slave_options[:on_master]
        
        ids = [ids].flatten
        ids.each do |rec_id|
          return master_connection if above_checkpoint?(id)
        end
        return slave_connection
      end
      
      def master_connection
        connection
      end
      
      def slave_connection
        @@slave_class.connection
      end
      
      def above_checkpoint?(id)
        id > checkpoint_value
      end
      
      def checkpoint_value
        return @@checkpoint_value if @@checkpoint_value
        @@checkpoint_value = find_checkpoint_value
      end
      
      def find_checkpoint_value
        run_on_connection(slave_connection) { maximum(primary_key) }
      end
      
    end
  end
end

ActiveRecord::Base.send(:include, SmartSlaves)
