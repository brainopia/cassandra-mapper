class Cassandra::Mapper::Data
  class Insert < Request
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
        next unless value
        converted[field] = Cassandra::Mapper::Convert.round config.types[field], value
      end
    end
  end
end
