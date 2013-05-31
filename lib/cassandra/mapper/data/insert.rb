class Cassandra::Mapper::Data
  class Insert < Request
    def initialize(_config, data)
      @request = data.dup
      super
    end

    def convert!(data)
      config.before_insert.each {|it| it.call data }
      super
    end

    def return!
      converted.tap do |data|
        config.after_insert.each {|it| it.call data }
      end
    end

    private

    def converted
      @request.each_with_object({}) do |(field, value), converted|
        next unless value
        converted[field] = Cassandra::Mapper::Convert.round config.type(field), value
      end
    end
  end
end
