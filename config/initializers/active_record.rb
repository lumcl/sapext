class ActiveRecord::Base
  self.primary_key = :uuid
  #connection.raw_connection.recv_timeout = 10
  #connection.raw_connection.send_timeout = 5

  before_create :assign_uuid

  def assign_uuid
    if attributes.include?('uuid')
      self.uuid = UUID.new.generate(:compact) if uuid.blank?
    end
  end

  def self.connection_test
    begin
      self.find_by_sql ('select sysdate from dual')
    rescue
      begin
        self.connection.reconnect!
      rescue Exception => exception
        Mail.defaults do
          delivery_method :smtp, address: '172.91.1.253', port: 25
        end
        Mail.deliver do
          from 'lum.cl@l-e-i.com'
          to 'lum.cl@l-e-i.com'
          subject 'IPQCWEB OCI ERROR'
          body exception.inspect
        end
      end
    end
  end

end