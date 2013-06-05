class Cassandra::Mapper
  BATCH_SIZE = 500

  def insert(hash)
    data = Data::Insert.new config, hash
    keyspace.insert table, data.packed_keys, data.columns
    data.return!
  end

  def remove(hash)
    data = Data::Remove.new config, hash
    keyspace.remove table, data.packed_keys, data.columns
    data.return!
  end

  def get(query, slice={})
    request  = Data::Request.new config, query
    columns  = columns_for request, slice
    response = Data::Response.new config, request.keys, columns
    response.unpack
  end

  def one(keys, filter={})
    get(keys, filter).first
  end

  def each(&block)
    keyspace.each_key table do |packed_keys|
      keys = unpack_keys packed_keys
      get(keys).each &block
    end
  end

  def all
    to_enum.to_a
  end

  private

  def columns_for(request, filter)
    query = request.query(filter.dup).merge! count: BATCH_SIZE
    columns = keyspace.get table, request.packed_keys, query
    columns ||= {}
    if columns.size == BATCH_SIZE
      filter[:start] = { slice: :after, subkey: columns.keys.last }
      columns.merge! columns_for(request, filter)
    end
    columns
  end

  def unpack_keys(packed_keys)
    keys = packed_keys.split Data::Request::KEY_SEPARATOR
    keys = Hash[config.key.zip(keys)]
    keys.each do |field, value|
      keys[field] = Convert.from config.type(field), value
    end
  end
end
