class Cassandra::Mapper
  class << self
    attr_writer :server

    def server
      @server || '127.0.0.1:9160'
    end
  end

  def server
    self.class.server
  end
end
