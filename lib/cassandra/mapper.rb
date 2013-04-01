require 'bigdecimal'
require 'time'
require 'cassandra'

class Cassandra::Mapper
  require_relative 'mapper/convert'
  require_relative 'mapper/request_data'
  require_relative 'mapper/insert_data'
  require_relative 'mapper/response_data'

  require_relative 'mapper/extend/schema'
  require_relative 'mapper/extend/migrate'
  require_relative 'mapper/extend/queries'

  require_relative 'mapper/utility'
  require_relative 'mapper/utility/delegate_keys'
  require_relative 'mapper/utility/config'
  require_relative 'mapper/utility/store_instances'

  extend Utility::StoreInstances

  attr_reader :table, :config

  def initialize(keyspace, table, &block)
    @keyspace = keyspace.to_s
    @table    = table.to_s
    @config   = Utility::Config.new(&block)
  end

  def keyspace
    Thread.current["keyspace_#@keyspace"] ||= Cassandra.new "#{@keyspace}_#{env}"
  end
end
