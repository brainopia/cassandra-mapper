class Cassandra::Mapper
  BATCH_SIZE = 100

  def insert(hash)
    data = Data::Insert.new config, hash
    keyspace.insert table, data.packed_keys, data.columns
    data.converted
  end

  def get(query)
    request  = Data::Request.new config, query
    columns  = columns_for request
    response = Data::Response.new config, request.keys, columns
    response.unpack
  end

  def one(keys)
    get(keys).first
  end

  def each(&block)
    keyspace.each_key table do |packed_keys|
      keys = unpack_keys packed_keys
      get(keys).each &block
    end
  end

  private

  def columns_for(request, offset=nil)
    columns = keyspace.get table, request.packed_keys, request.query(offset)
    columns ||= {}
    if columns.size == BATCH_SIZE
      columns.merge! columns_for(request, columns.keys.last)
    end
    columns
  end

  def unpack_keys(packed_keys)
    keys = packed_keys.split Data::Request::KEY_SEPARATOR
    keys = Hash[config.key.zip(keys)]
    keys.each do |field, value|
      keys[field] = Convert.from config.types[field], value
    end
  end
end
