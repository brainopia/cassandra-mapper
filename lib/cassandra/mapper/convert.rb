class Cassandra::Mapper
  module Convert
    extend self

    TYPES = {
      nil =>    'UTF8Type',
      uuid:     'TimeUUIDType',
      integer:  'Int32Type'
    }

    def type(symbol)
      TYPES[symbol]
    end

    def to(type, value)
      send "to_#{type}", value
    end

    def from(type, value)
      send "from_#{type}", value
    end

    private

    def to_(value)
      value.to_s
    end

    def from_(value)
      value
    end

    def to_uuid(value)
      SimpleUUID::UUID.new(value).to_s
    end

    def from_uuid(value)
      SimpleUUID::UUID.new value
    end

    def to_integer(value)
      [value].pack('N')
    end

    def from_integer(value)
      value.unpack('N').first
    end

    def to_boolean(value)
      value ? 'on' : 'off'
    end

    def from_boolean(value)
      value == 'on' ? true : false
    end

    def to_time(value)
      value.to_s
    end

    def from_time(value)
      Time.parse value
    end

    def to_decimal(value)
      value.to_s
    end

    def from_decimal(value)
      BigDecimal value
    end

    def to_json(value)
      MultiJson.dump value
    end

    def from_json(value)
      MultiJson.load value
    end
  end
end
