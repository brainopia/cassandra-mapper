class Cassandra::Mapper
  class InsertData < RequestData
    def convert!(data)
      config.before.call data if config.before
      super
    end
  end
end