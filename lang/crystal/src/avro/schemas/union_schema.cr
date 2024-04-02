require "./abstract_schema"

module Avro::Schemas 
  class UnionSchema < AbstractSchema
    getter :schemas
    @schemas : Array(Avro::Schemas::AbstractSchema)

    def initialize(schemas, names : Hash(String, Avro::Schemas::AbstractSchema)? = nil, default_namespace : String? = nil)
      super("union")

      @schemas = schemas.each_with_object([] of Avro::Schemas::AbstractSchema) do |schema, schema_objects|
        new_schema = subparse(schema, names, default_namespace)
        ns_type = new_schema.type

        if VALID_TYPES.includes?(ns_type) &&
           !NAMED_TYPES.includes?(ns_type) &&
           schema_objects.any? { |o| o.type == ns_type }
          raise SchemaParseError.new("#{ns_type} is already in Union")
        elsif ns_type == :union
          raise SchemaParseError.new("Unions cannot contain other unions")
        else
          schema_objects << new_schema
        end
      end
    end

    def to_avro(names = Set.new)
      schemas.map { |schema| schema.to_avro(names) }
    end
  end
end
