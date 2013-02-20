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
  end
end
