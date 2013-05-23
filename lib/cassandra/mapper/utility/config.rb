module Cassandra::Mapper::Utility
  class Config
    extend DelegateKeys
    delegate_keys :@options, :key, :subkey, :types, :before

    def initialize(&block)
      @options = DSL.run &block
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
        @options[:key] = fields
      end

      def subkey(*fields)
        @options[:subkey] = fields
      end

      def type(field, type)
        @options[:types][field] = type
      end

      def before(&block)
        @options[:before] = block
      end
    end
  end
end
