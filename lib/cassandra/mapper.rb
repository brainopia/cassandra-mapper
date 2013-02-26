require 'bigdecimal'
require 'time'
require 'cassandra'

class Cassandra::Mapper
  require_relative 'mapper/config'
  require_relative 'mapper/convert'
  require_relative 'mapper/request_data'
  require_relative 'mapper/response_data'

  attr_reader :keyspace, :table, :config

  def initialize(keyspace, table, &block)
    @keyspace = keyspace.to_s
    @table    = table.to_s
    @config   = Config.new(&block)
  end

  def keyspace
    Thread.current["keyspace_#@keyspace"] ||= Cassandra.new @keyspace
  end

  def insert(data)
    data = RequestData.new config, data
    keyspace.insert table, data.packed_keys, data.columns
  end

  def get(query)
    request  = RequestData.new config, query
    columns  = keyspace.get table, request.packed_keys, request.query
    response = ResponseData.new config, request.keys, columns
    response.unpack
  end

  def one(keys)
    get(keys).first
  end

  def migrate
    subkey_types = config.subkey.map do |it|
      Convert.type config.types[it]
    end

    # field subkey
    subkey_types.push Convert::TEXT_TYPE

    keyspace.add_column_family Cassandra::ColumnFamily.new \
      keyspace: keyspace.keyspace,
      name: table,
      comparator_type: "CompositeType(#{subkey_types.join ','})"
  end
end
