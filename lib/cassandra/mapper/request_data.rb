class Cassandra::Mapper
  class RequestData
    KEY_SEPARATOR = '##'

    attr_reader :keys, :subkeys, :data, :config

    def initialize(config, data)
      @config  = config
      @data    = convert! data.dup
      @keys    = extract! :key
      @subkeys = extract! :subkey
    end

    def packed_keys
      keys.join KEY_SEPARATOR
    end

    def columns
      fields = data.empty? ? { '' => '' } : data
      fields.each_with_object({}) do |(field, value), columns|
        columns[composite *subkeys, field.to_s] = value
      end
    end

    def query
      return if subkeys.all? &:empty?
      { start:  composite(*subkeys),
        finish: composite(*subkeys, slice: :after) }
    end

    private

    def composite(*args)
      Cassandra::Composite.new *args
    end

    def extract!(option)
      config.send(option).map {|it| data.delete(it).to_s }
    end

    def convert!(data)
      data.each do |field, value|
        data[field] = Convert.to config.types[field], value
      end
    end
  end
end
