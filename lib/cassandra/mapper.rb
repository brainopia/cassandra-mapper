require 'bigdecimal'
require 'yaml'
require 'json'
require 'time'
require 'base64'
require 'murmurhash3'
require 'cassandra'

Cassandra::THRIFT_DEFAULTS.merge! \
  connect_timeout: 1, timeout: 1

class Cassandra::Mapper
  require_relative 'extensions/time'
  require_relative 'extensions/simple_uuid'

  require_relative 'mapper/convert'
  require_relative 'mapper/data/request'
  require_relative 'mapper/data/insert'
  require_relative 'mapper/data/remove'
  require_relative 'mapper/data/response'

  require_relative 'mapper/extend/server'
  require_relative 'mapper/extend/schema'
  require_relative 'mapper/extend/migrate'
  require_relative 'mapper/extend/queries'
  require_relative 'mapper/extend/data'

  require_relative 'mapper/utility/hash'
  require_relative 'mapper/utility/delegate_keys'
  require_relative 'mapper/utility/config'
  require_relative 'mapper/utility/store_instances'

  extend Utility::StoreInstances

  attr_reader :table, :config

  def initialize(keyspace, table, &block)
    @keyspace = keyspace.to_s
    @table    = table.to_s[0...48]
    @config   = Utility::Config.new(&block)
  end

  def keyspace
    Thread.current["keyspace_#{keyspace_name}"] ||= Cassandra.new keyspace_name, server
  end

  def keyspace_name
    "#{@keyspace}_#{env}"
  end

  def keyspace_base
    @keyspace
  end
end
