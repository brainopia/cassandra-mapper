class Cassandra::Mapper::Data
  class Request
    KEY_SEPARATOR = '##'

    attr_reader :keys, :subkeys, :data, :config

    def initialize(config, data)
      @config  = config
      @data    = convert! data.dup
      @keys    = extract! :key
      @subkeys = extract! :subkey
    end

    def packed_keys
      keys.join(KEY_SEPARATOR).force_encoding('binary')
    end

    def columns
      fields = data.empty? ? { '' => '' } : data
      fields.each_with_object({}) do |(field, value), columns|
        columns[composite *subkeys, field.to_s] = value
      end
    end

    def query(filter)
      case
      when !filter.empty?
        filter[:start]  &&= create_filter filter[:start]
        filter[:finish] &&= create_filter filter[:finish]
        filter
      when !subkeys.all?(&:empty?)
        { start:  composite(*subkeys),
          finish: composite(*subkeys, slice: :after) }
      end
    end

    private

    def create_filter(filter)
      slice     = filter.delete :slice
      composite = filter.delete :subkey

      parts = composite ? composite.parts : extract!(:subkey, convert!(filter))
      composite(*parts, slice: slice)
    end

    def composite(*args)
      Cassandra::Composite.new *args
    end

    def extract!(option, from=data)
      config.send(option).to_a.map {|it| from.delete(it).to_s }
    end

    def convert!(data)
      data.delete_if {|_, value| value.nil? }
      data.each do |field, value|
        data[field] = Cassandra::Mapper::Convert.to config.types[field], value
      end
    end
  end
end
