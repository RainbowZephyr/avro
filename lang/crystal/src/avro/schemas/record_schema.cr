require "./named_schema"

module Avro::Schemas 
  class RecordSchema < NamedSchema
    getter :fields, :doc

    @fields = Array(Field).new

    def self.make_field_objects(field_data, names : Hash(String, Avro::Schemas::AbstractSchema), namespace : String? = nil)  : Array(Field)
      field_objects = Array(Field).new
      field_names = Set(String).new
      alias_names = Set(String).new

      field_data.each do |field|
        puts typeof(field)
        puts field
        if field.responds_to?(:[])
          hash = field.as_h
          type : String = String.from_json(hash["type"].to_json)
          name = String.from_json(hash["name"].to_json)
          default = hash.has_key?("default") ? String.from_json(hash["default"].to_json) : "no_default"
          order = String.from_json(hash.fetch("order", "null").to_json)
          doc = String.from_json(hash.fetch("doc", "null").to_json)
          aliases = Array(String).from_json(hash.fetch("aliases", "null").to_json)
          new_field = Field.new(type, name, default, order, names, namespace, doc, aliases)
          # make sure field name has not been used yet
          if field_names.includes?(new_field.name)
            raise SchemaParseError.new("Field name #{new_field.name.inspect} is already in use")
          end
          field_names << new_field.name
          # make sure alias has not be been used yet
          if new_field.aliases && alias_names.intersect?(new_field.aliases.to_set)
            raise SchemaParseError.new("Alias #{(alias_names & new_field.aliases).to_a} already in use")
          end
          alias_names.merge(new_field.aliases) if new_field.aliases
        else
          raise SchemaParseError.new("Not a valid field: #{field}")
        end
        field_objects << new_field
      end

      return field_objects
    end

    def initialize(name : String, namespace : String, fields : Array(JSON::Any), names : Hash(String, Avro::Schemas::AbstractSchema)? = nil, schema_type : String = "record", doc : String? = nil, aliases : Array(String)? = nil)
      @type_sym = schema_type
      if schema_type == "request"
        @namespace = namespace
        @type = schema_type
        @name = name
        # @doc = nil
      else
        super(schema_type, name, namespace, names, doc, nil, aliases)
      end

      if fields
        @fields = RecordSchema.make_field_objects(fields, names, self.namespace)
      else
        @fields = Array(Field).new
      end
    end

    def fields_hash
      @fields_hash ||= fields.inject({} of String => String) { |hsh, field| hsh[field.name] = field; hsh }
    end

    def fields_by_alias
      @fields_by_alias ||= fields.each_with_object({} of String => String) do |field, hash|
        if field.aliases
          field.aliases.each do |a|
            hash[a] = field
          end
        end
      end
    end

    def to_avro(names = Set.new)
      hsh = super
      return hsh unless hsh.is_a?(Hash)
      hsh["fields"] = @fields.map { |f| f.to_avro(names) }
      if type_sym == :request
        hsh["fields"]
      else
        hsh
      end
    end
  end
end
