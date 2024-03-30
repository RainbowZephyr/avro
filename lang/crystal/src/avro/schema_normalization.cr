require "json"

module Avro
  class SchemaNormalization
    def self.to_parsing_form(schema)
      new.to_parsing_form(schema)
    end

    def initialize
      @processed_names = [] of String
    end

    def to_parsing_form(schema)
      JSON.generate(normalize_schema(schema))
    end

    private def normalize_schema(schema)
      type = schema.type_sym.to_s

      if Schema::NAMED_TYPES.include?(type)
        if @processed_names.include?(schema.name)
          return schema.name
        else
          @processed_names << schema.name
        end
      end

      case type
      when *Schema::PRIMITIVE_TYPES
        type
      when "record"
        fields = schema.fields.map { |field| normalize_field(field) }

        normalize_named_type(schema, fields: fields)
      when "enum"
        normalize_named_type(schema, symbols: schema.symbols)
      when "fixed"
        normalize_named_type(schema, size: schema.size)
      when "array"
        { "type" => type, "items" => normalize_schema(schema.items) }
      when "map"
        { "type" => type, "values" => normalize_schema(schema.values) }
      when "union"
        if schema.schemas.nil?
          []
        else
          schema.schemas.map { |s| normalize_schema(s) }
        end
      else
        raise "unknown type #{type}"
      end
    end

    private def normalize_field(field)
      {
        "name" => field.name,
        "type" => normalize_schema(field.type)
      }
    end

    private def normalize_named_type(schema, attributes = {})
      name = Name.make_fullname(schema.name, schema.namespace)

      { "name" => name, "type" => schema.type_sym.to_s }.merge(attributes)
    end
  end
end
