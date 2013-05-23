module Cassandra::Mapper::Utility
  class Config
    extend DelegateKeys
    delegate_keys 'dsl.options', :key, :subkey, :types, :before, :after

    attr_reader :dsl

    def initialize(&block)
      @dsl = DSL.new &block
    end

    class DSL
      attr_reader :options

      def initialize(&block)
        @options = { types: {}, before: [], after: []}
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
        @options[:before].push block
      end

      def after(&block)
        @options[:after].push block
      end
    end
  end
end
