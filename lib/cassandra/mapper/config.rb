class Cassandra::Mapper
  class Config
    attr_reader :options

    def initialize(&block)
      @options = DSL.run &block
    end

    def key
      @options[:key]
    end

    def subkeys
      @options[:subkeys]
    end

    def types
      @options[:types]
    end

    class DSL
      def self.run(&block)
        new(&block).options
      end

      attr_reader :options

      def initialize(&block)
        @options = { types: {}}
        instance_eval &block
      end

      def key(*fields)
        @options[:key]     = fields.shift
        @options[:subkeys] = fields
      end

      def types(hash)
        @options[:types] = hash
      end
    end
  end
end
