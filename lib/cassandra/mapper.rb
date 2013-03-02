require 'bigdecimal'
require 'time'
require 'cassandra'

class Cassandra::Mapper
  require_relative 'mapper/config'
  require_relative 'mapper/convert'
  require_relative 'mapper/request_data'
  require_relative 'mapper/response_data'
  require_relative 'mapper/extend/migrate'
  require_relative 'mapper/extend/queries'

  attr_reader :table, :config

  @all = []
  singleton_class.send :attr_reader, :all

  def initialize(keyspace, table, &block)
    @keyspace = keyspace.to_s
    @table    = table.to_s
    @config   = Config.new(&block)

    self.class.all << self
  end

  def keyspace
    Thread.current["keyspace_#@keyspace"] ||= Cassandra.new @keyspace
  end
end
