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

  def one(keys, slice={})
    get(keys, slice).first
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

  def columns_for(request, slice)
    columns = keyspace.get table, request.packed_keys, request.query(slice)
    columns ||= {}
    if columns.size == BATCH_SIZE
      slice[:start] = [*columns.keys.last.parts, slice: :after]
      columns.merge! columns_for(request, slice)
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
