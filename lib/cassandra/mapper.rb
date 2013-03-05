require 'bigdecimal'
require 'time'
require 'cassandra'

class Cassandra::Mapper
  require_relative 'mapper/convert'
  require_relative 'mapper/request_data'
  require_relative 'mapper/response_data'
  require_relative 'mapper/extend/migrate'
  require_relative 'mapper/extend/queries'
  require_relative 'mapper/utility/config'
  require_relative 'mapper/utility/instances'

  extend Instances

  attr_reader :table, :config

  def initialize(keyspace, table, &block)
    @keyspace = keyspace.to_s
    @table    = table.to_s
    @config   = Config.new(&block)
  end

  def keyspace
    Thread.current["keyspace_#@keyspace"] ||= Cassandra.new @keyspace
  end
end
