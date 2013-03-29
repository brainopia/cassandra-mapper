class Cassandra::Mapper
  BATCH_SIZE = 100

  def insert(hash)
    data = InsertData.new config, hash
    keyspace.insert table, data.packed_keys, data.columns
    hash
  end

  def get(query)
    request  = RequestData.new config, query
    columns  = columns_for request
    response = ResponseData.new config, request.keys, columns
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
    columns.concat make(request, columns.keys.last) if columns.size == BATCH_SIZE
    columns
  end

  def unpack_keys(packed_keys)
    keys = packed_keys.split RequestData::KEY_SEPARATOR
    keys = Hash[config.key.zip(keys)]
    keys.each do |field, value|
      keys[field] = Convert.from config.types[field], value
    end
  end
end
