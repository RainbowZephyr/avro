require "./schema_parse_error"

module Avro 
    class UnknownSchemaError < SchemaParseError
        property type_name : String
        property default_namespace : String
    
        def initialize(type, default_namespace)
          @type_name = type
          @default_namespace = default_namespace
          super("#{type.inspect} is not a schema we know about.")
        end
      end
end