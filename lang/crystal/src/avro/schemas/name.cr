
module Avro::Schemas::Name
    def self.extract_namespace(name : String, namespace : String?) : Tuple(String, String?)
      parts = name.split('.')
      if parts.size > 1
        namespace, name = parts[0..-2].join('.'), parts.last
      end
      return name, namespace
    end

    # Add a new schema object to the names dictionary (in place).
    def self.add_name(names : Hash(String, Avro::Schemas::AbstractSchema), new_schema : Avro::Schemas::AbstractSchema)
      new_fullname = new_schema.fullname
      if Avro::Schemas::AbstractSchema::VALID_TYPES.includes?(new_fullname)
        raise SchemaParseError.new("#{new_fullname} is a reserved type name.")
      elsif names.nil?
        names = Hash(String, Avro::Schemas::AbstractSchema).new
      elsif names.has_key?(new_fullname)
        raise SchemaParseError.new("The name \"#{new_fullname}\" is already in use.")
      end

      names[new_fullname] = new_schema
      names
    end

    def self.make_fullname(name : String, namespace : String?) : String
      if !name.includes?('.') && !namespace.nil?
        namespace + '.' + name
      else
        name
      end
    end
  end