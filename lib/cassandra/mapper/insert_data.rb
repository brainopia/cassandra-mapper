class Cassandra::Mapper
  class InsertData < RequestData
    def initialize(_config, data)
      @request = data.dup
      super
    end

    def convert!(data)
      config.before.call data if config.before
      super
    end

    def converted
      @request.each_with_object({}) do |(field, value), converted|
        converted[field] = Convert.round config.types[field], value
      end
    end
  end
end
