require "./named_schema"

module Avro::Schemas 
    class EnumSchema < NamedSchema
        SYMBOL_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/
  
        getter :symbols, :doc, :default
  
        def initialize(name : String, space : String, symbols : Array(String), names : Hash(String, Avro::Schemas::AbstractSchema)? = nil, doc : String? = nil, default : JSON::Any? = nil, aliases : Array(String)? = nil)
          if symbols.uniq.size < symbols.size
            fail_msg = "Duplicate symbol: #{symbols}"
            raise Avro::SchemaParseError.new(fail_msg)
          end
  
          if !Avro.disable_enum_symbol_validation
            invalid_symbols = symbols.select { |symbol| symbol !~ SYMBOL_REGEX }
  
            if invalid_symbols.any?
              raise SchemaParseError.new("Invalid symbols for #{name}: #{invalid_symbols.join(", ")} don't match #{SYMBOL_REGEX.inspect}")
            end
          end
  
          if default && !symbols.includes?(default)
            raise Avro::SchemaParseError.new("Default '#{default}' is not a valid symbol for enum #{name}")
          end
  
          super("enum", name, space, names, doc, nil, aliases)
          @default = default
          @symbols = symbols
        end
  
        def to_avro(_names = Set.new)
          avro = super
          if avro.is_a?(Hash)
            avro["symbols"] = symbols
            avro["default"] = default if default
          end
          avro
        end
      end
  


end