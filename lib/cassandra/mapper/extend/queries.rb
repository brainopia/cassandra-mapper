class Cassandra::Mapper
  def insert(hash)
    data = InsertData.new config, hash
    keyspace.insert table, data.packed_keys, data.columns
    hash
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

  def each(&block)
    keyspace.each table do |key, columns|
      keys = key.split RequestData::KEY_SEPARATOR
      response = ResponseData.new config, keys, columns
      response.unpack.each &block
    end
  end
end
