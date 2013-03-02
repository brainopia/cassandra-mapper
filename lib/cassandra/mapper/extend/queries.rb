class Cassandra::Mapper
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
end
