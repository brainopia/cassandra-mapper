module Cassandra::Mapper::Convert
  extend self

  TEXT_TYPE = 'UTF8Type'
  MAX = {
    time: Time.new(2038),
    integer: 2147483647
  }
  TYPES = {
    uuid:     'TimeUUIDType',
    integer:  'Int32Type',
    time:     'DateType'
  }

  def max(type)
    MAX[type] or raise ArgumentError, "unsupported type: #{type}"
  end

  def type(symbol)
    TYPES.fetch symbol, TEXT_TYPE
  end

  def to(type, value)
    send "to_#{type}", value
  end

  def from(type, value)
    send "from_#{type}", value
  end

  def round(type, value)
    from type, to(type, value)
  end

  private

  def to_(value)
    value.to_s
  end

  def from_(value)
    value.force_encoding Encoding::UTF_8 if value
  end

  def to_uuid(value)
    if value.is_a? String
      value = Time.parse time
    end

    if value.is_a? Time
      value.uuid ||= SimpleUUID::UUID.new value
      value = value.uuid
    end

    unless value.is_a? SimpleUUID::UUID
      raise ArgumentError, value.inspect
    end

    value.to_s
  end

  def from_uuid(value)
    SimpleUUID::UUID.new(value).to_time
  end

  def to_integer(value)
    [value.to_i].pack('N')
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
    value = Time.parse value if value.is_a? String
    value = value.to_time if value.is_a? Date
    [(value.to_f * 1000).to_i].pack('Q>')
  end

  def from_time(value)
    Time.at(value.unpack('Q>').first.to_f / 1000)
  end

  def to_decimal(value)
    value.to_s
  end

  def from_decimal(value)
    BigDecimal value
  end

  def to_json(value)
    JSON.dump value
  end

  def from_json(value)
    JSON.load value
  end

  def to_float(value)
    value.to_s
  end

  def from_float(value)
    value.to_f
  end

  def to_yaml(value)
    YAML.dump value
  end

  def from_yaml(value)
    YAML.load value
  end
end
