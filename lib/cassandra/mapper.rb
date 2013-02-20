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
        keyspace.insert table, key, composite(subkeys, field) => value
      end
    end
  end

  def get(keys)
    keys    = convert keys
    key     = keys.delete config.key
    subkeys = config.subkeys.map {|it| keys.delete(it).to_s }

    keyspace.get table, key,
      start: Cassandra::Composite.new(*subkeys),
      finish: Cassandra::Composite.new(*subkeys)
  end

  def migrate
    subkey_types = config.subkeys.map do |it|
      Convert.type config.types[it]
    end

    keyspace.add_column_family Cassandra::ColumnFamily.new \
      keyspace: keyspace.keyspace,
      name: table.to_s,
      comparator_type: "CompositeType(#{subkey_types.join ','},UTF8Type)"
  end

  private

  def convert(data)
    data = data.dup
    data.each do |field, value|
      data[field] = Convert.to config.types[field], value
    end
  end

  def composite(subkeys, field)
    Cassandra::Composite.new *subkeys, field.to_s
  end
end
