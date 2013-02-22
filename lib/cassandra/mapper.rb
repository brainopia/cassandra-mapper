require 'bigdecimal'
require 'time'
require 'cassandra'

class Cassandra::Mapper
  require_relative 'mapper/config'
  require_relative 'mapper/convert'

  attr_reader :keyspace, :table, :config

  def initialize(keyspace, table, &block)
    @keyspace = Cassandra.new keyspace.to_s
    @table    = table
    @config   = Config.new(&block)
  end

  def insert(data)
    data    = convert data
    key     = data.delete config.key
    subkeys = config.subkeys.map {|it| data.delete(it).to_s }

    data = { '' => '' } if data.empty?

    keyspace.batch do
      data.each do |field, value|
        keyspace.insert table, key, composite(*subkeys, field.to_s) => value
      end
    end
  end

  def one(keys)
    get(keys).first
  end

  def get(keys)
    keys    = convert keys
    key     = keys.delete config.key
    subkeys = config.subkeys.map {|it| keys.delete(it).to_s }

    if subkeys.any? {|it| not it.empty? }
      options = {
        start: composite(*subkeys),
        finish: composite(*subkeys, slice: :after)
      }
    end

    result = keyspace.get table, key, options

    unless result.empty?
      slices = result.each_with_object({}) do |(composite, value), hash|
        slice = hash[composite[0..-2]] ||= {}
        field = composite[-1]
        slice[field.to_sym] = value unless field.empty?
      end

      slices.map do |subkeys, fields|
        fields.merge! Hash[config.subkeys.zip(subkeys)]
        fields[config.key] = key
        unconvert fields
      end
    end
  end

  def migrate
    subkey_types = config.subkeys.map do |it|
      Convert.type config.types[it]
    end

    # field subkey
    subkey_types.push Convert::TEXT_TYPE

    keyspace.add_column_family Cassandra::ColumnFamily.new \
      keyspace: keyspace.keyspace,
      name: table.to_s,
      comparator_type: "CompositeType(#{subkey_types.join ','})"
  end

  private

  def convert(data)
    data = data.dup
    data.each do |field, value|
      data[field] = Convert.to config.types[field], value
    end
  end

  def unconvert(data)
    data.each do |field, value|
      data[field] = Convert.from config.types[field], value
    end
  end

  def composite(*args)
    Cassandra::Composite.new *args
  end
end
