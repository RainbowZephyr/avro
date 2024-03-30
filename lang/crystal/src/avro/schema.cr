require "set"
require "json"
require "./avro_error"

module Avro
  class Schema
    PRIMITIVE_TYPES = Set.new(%w[null boolean string bytes int long float double])
    NAMED_TYPES     = Set.new(%w[fixed enum record error])
    VALID_TYPES     = PRIMITIVE_TYPES + NAMED_TYPES + Set.new(%w[array map union request])

    # PRIMITIVE_TYPES_SYM = Set.new(PRIMITIVE_TYPES.map(&.to_sym))
    # NAMED_TYPES_SYM = Set.new(NAMED_TYPES.map(&.to_sym))
    # VALID_TYPES_SYM = Set.new(VALID_TYPES.map(&.to_sym))

    NAME_REGEX = /^([A-Za-z_][A-Za-z0-9_]*)(\.([A-Za-z_][A-Za-z0-9_]*))*$/

    INT_MIN_VALUE  = -(1 << 31)
    INT_MAX_VALUE  = (1 << 31) - 1
    LONG_MIN_VALUE = -(1 << 63)
    LONG_MAX_VALUE = (1 << 63) - 1

    DEFAULT_VALIDATE_OPTIONS = {recursive: true, encoded: false}
    DECIMAL_LOGICAL_TYPE     = "decimal"

    def self.parse(json_string : String)
      real_parse(JSON.parse(json_string), {} of String => Avro::Schema)
    end

    def self.real_parse(json_obj : JSON::Any, names : Hash(String, Avro::Schema), default_namespace : String? = nil)
      begin
        # if json_obj.is_a?(Hash)
        hash = json_obj.as_h
        type : String = hash["type"].to_s
        logical_type = hash["logicalType"].to_s
        raise SchemaParseError.new("No 'type' property: #{json_obj}") if type.nil?

        unless VALID_TYPES.includes?(type)
          raise SchemaParseError.new("Unknown type: #{type}")
        end

        precision : Int64?
        scale : Int64?
        size : Int64?
        namespace : String?
        aliases : Array(String)?
        symbols : Array(String)?
        doc : String?

        # type_sym = type.to_sym
        if PRIMITIVE_TYPES.includes?(type)
          puts "TYPE #{type}"
          case type
          when "bytes"
            precision = Int64.from_json(hash["precision"].to_json)
            scale = Int64.from_json(hash["scale"].to_json)
            return BytesSchema.new(type, logical_type, precision, scale)
          else
            return PrimitiveSchema.new(type, logical_type)
          end
        elsif NAMED_TYPES.includes?(type)
          name = hash["name"].to_s
          if !Avro.disable_schema_name_validation && name !~ NAME_REGEX
            raise SchemaParseError.new("Name #{name} is invalid for type #{type}!")
          end

          namespace = hash.includes?("namespace") ? hash["namespace"].to_s : default_namespace
          aliases = Array(String).from_json(hash["aliases"].to_json)
          fields : Array(JSON::Any)

          case type
          when "fixed"
            size = Int64.from_json(hash["size"].to_json)
            precision = Int64.from_json(hash["precision"].to_json)
            scale = Int64.from_json(hash["scale"].to_json)
            return FixedSchema.new(name, namespace, size, names, logical_type, aliases, precision, scale)
          when "enum"
            symbols = Array(String).from_json(hash["symbols"].to_json)
            doc = hash["doc"].to_s
            default = hash["default"]
            return EnumSchema.new(name, namespace.as(String), symbols, names, doc, default, aliases)
          when "record", "error"
            fields = Array(JSON::Any).from_json(hash["fields"].to_json)
            doc = hash["doc"].to_s
            return RecordSchema.new(name, namespace.as(String), fields, names, type, doc, aliases)
          else
            raise SchemaParseError.new("Unknown named type: #{type}")
          end
        else
          case type_sym
          when "array"
            return ArraySchema.new(hash["items"], names, default_namespace)
          when "map"
            return MapSchema.new(hash["values"], names, default_namespace)
          else
            raise SchemaParseError.new("Unknown Valid Type: #{type}")
          end
        end
      rescue e : TypeCastError
      end

      begin
        # elsif json_obj.is_a?(Array)
        array = json_obj.as_a
        return UnionSchema.new(array, names, default_namespace)
      rescue e : TypeCastError
      end

      if PRIMITIVE_TYPES.includes?(json_obj)
        return PrimitiveSchema.new(json_obj)
      else
        raise UnknownSchemaError.new(json_obj, default_namespace)
      end
    end

    def self.validate(expected_schema, logical_datum, options = DEFAULT_VALIDATE_OPTIONS)
      SchemaValidator.validate!(expected_schema, logical_datum, options)
      true
    rescue SchemaValidator::ValidationError
      false
    end

    getter :type, :logical_type

    # @logical_type : Hash(String, String)

    def initialize(@type : String, @logical_type : String? = nil)
      # @type_sym = type.is_a?(Symbol) ? type : type.to_sym
      @logical_type = logical_type
      @type_adapter = ""
    end

    # def type
    #   @type_sym.to_s
    # end

    def type_adapter
      @type_adapter ||= LogicalTypes.type_adapter(type, logical_type, self) || LogicalTypes::Identity
    end

    def md5_fingerprint
      parsing_form = SchemaNormalization.to_parsing_form(self)
      Digest::MD5.hexdigest(parsing_form).to_i(16)
    end

    def sha256_fingerprint
      parsing_form = SchemaNormalization.to_parsing_form(self)
      Digest::SHA256.hexdigest(parsing_form).to_i(16)
    end

    CRC_EMPTY = 0xc15d213aa4d7a795

    @@fp_table = nil?

    def init_fp_table
      @@fp_table = Array.new(256)
      256.times do |i|
        fp = i
        8.times do
          fp = (fp >> 1) ^ (CRC_EMPTY & -(fp & 1))
        end
        @@fp_table[i] = fp
      end
    end

    def crc_64_avro_fingerprint
      parsing_form = Avro::SchemaNormalization.to_parsing_form(self)
      data_bytes = parsing_form.unpack("C*")

      init_fp_table unless @@fp_table

      fp = CRC_EMPTY
      data_bytes.each do |b|
        fp = (fp >> 8) ^ @@fp_table[(fp ^ b) & 0xff]
      end
      fp
    end

    SINGLE_OBJECT_MAGIC_NUMBER = [0xC3, 0x01].freeze

    def single_object_encoding_header
      [SINGLE_OBJECT_MAGIC_NUMBER, single_object_schema_fingerprint].flatten
    end

    def single_object_schema_fingerprint
      working = crc_64_avro_fingerprint
      bytes = Array.new(8)
      8.times do |i|
        bytes[i] = (working & 0xff)
        working = working >> 8
      end
      bytes
    end

    def read?(writers_schema)
      SchemaCompatibility.can_read?(writers_schema, self)
    end

    def be_read?(other_schema)
      other_schema.read?(self)
    end

    def mutual_read?(other_schema)
      SchemaCompatibility.mutual_read?(other_schema, self)
    end

    def ==(other)
      other.is_a?(Schema) && type_sym == other.type_sym
    end

    def hash(_seen = nil?)
      type_sym.hash
    end

    def subparse(json_obj : String, names : Hash(String, Avro::Schema)? = nil, namespace : String? = nil) : String
      if names
        fullname = Name.make_fullname(json_obj, namespace)
        return names[fullname] if names.includes?(fullname)
      end
      begin
        Schema.real_parse(json_obj, names, namespace)
      rescue e
        raise e if e.is_a?(SchemaParseError)
        raise SchemaParseError.new("Sub-schema for #{self.class.name} not a valid Avro schema. Bad schema: #{json_obj}")
      end
    end

    def to_avro(_names = nil?)
      props = {"type" => type}
      props["logicalType"] = logical_type if logical_type
      props
    end

    def to_s
      MultiJson.dump(to_avro)
    end

    private def validate_aliases!
      unless aliases.nil? ||
             (aliases.is_a?(Array) && aliases.as(Array(String)).all? { |a| a.is_a?(String) })
        raise Avro::SchemaParseError.new("Invalid aliases value #{aliases.inspect} for #{type} #{name}. Must be an array of strings.")
      end
    end

    class NamedSchema < Schema
      getter :name, :namespace, :aliases

      def initialize(type : String, name : String, namespace : String? = nil, names : Hash(String, Avro::Schema)? = nil, doc : String? = nil, logical_type : String? = nil, aliases : Array(String)? = nil)
        super(type, logical_type)
        @name, @namespace = Name.extract_namespace(name, namespace)
        @doc = doc
        @aliases = aliases
        validate_aliases! if aliases
        Name.add_name(names, self)
      end

      def to_avro(names = Set.new)
        if @name
          return fullname if names.includes?(fullname)
          names << fullname
        end
        props = {"name" => @name}
        props.merge!({"namespace" => @namespace}) if @namespace
        props["namespace"] = @namespace if @namespace
        props["doc"] = @doc if @doc
        props["aliases"] = aliases if aliases && aliases.any?
        super.merge(props)
      end

      def fullname
        @fullname ||= Name.make_fullname(@name, @namespace)
      end

      def fullname_aliases
        @fullname_aliases ||= if aliases
                                aliases.map { |a| Name.make_fullname(a, namespace) }
                              else
                                Array(String).new
                              end
      end

      def match_fullname?(name)
        name == fullname || fullname_aliases.includes?(name)
      end

      def match_schema?(schema)
        type_sym == schema.type_sym && match_fullname?(schema.fullname)
      end
    end

    class RecordSchema < NamedSchema
      getter :fields, :doc

      def self.make_field_objects(field_data, names, namespace = nil?) : Array(Field)
        field_objects = Array(Field).new
        field_names = Set(String).new
        alias_names = Set(String).new

        field_data.each do |field|
          if field.is_a?(Array)
            type : String = field["type"].to_s
            name = field["name"]
            default = field.key?("default") ? field["default"] : :no_default
            order = field["order"]
            doc = field["doc"]
            aliases = field["aliases"]
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

      def initialize(name : String, namespace : String, fields : Array(JSON::Any), names : Hash(String, Avro::Schema)? = nil, schema_type : String = "record", doc : String? = nil, aliases : Array(String)? = nil)
        if schema_type == "request"
          @type_sym = schema_type
          @namespace = namespace
          # @name = nil
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

    class ArraySchema < Schema
      getter :items

      def initialize(items, names = nil?, default_namespace = nil?)
        super(:array)
        @items = subparse(items, names, default_namespace)
      end

      def to_avro(names = Set.new)
        super.merge({"items" => items.to_avro(names)})
      end
    end

    class MapSchema < Schema
      getter :values

      def initialize(values, names = nil?, default_namespace = nil?)
        super(:map)
        @values = subparse(values, names, default_namespace)
      end

      def to_avro(names = Set.new)
        super.merge({"values" => values.to_avro(names)})
      end
    end

    class UnionSchema < Schema
      getter :schemas

      def initialize(schemas, names = nil?, default_namespace = nil?)
        super(:union)

        @schemas = schemas.each_with_object([] of Schema) do |schema, schema_objects|
          new_schema = subparse(schema, names, default_namespace)
          ns_type = new_schema.type_sym

          if VALID_TYPES.includes?(ns_type) &&
             !NAMED_TYPES.includes?(ns_type) &&
             schema_objects.any? { |o| o.type_sym == ns_type }
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

    class EnumSchema < NamedSchema
      SYMBOL_REGEX = /^[A-Za-z_][A-Za-z0-9_]*$/

      getter :symbols, :doc, :default

      def initialize(name : String, space : String, symbols : Array(String), names : Hash(String, Avro::Schema)? = nil, doc : String? = nil, default : JSON::Any? = nil, aliases : Array(String)? = nil)
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

    # Valid primitive types are in PRIMITIVE_TYPES.
    class PrimitiveSchema < Schema
      def initialize(type, logical_type = nil?)
        if PRIMITIVE_TYPES.includes?(type)
          super(type, logical_type)
        else
          raise AvroError.new("#{type} is not a valid primitive type.")
        end
      end

      def to_avro(names = nil?)
        hsh = super
        hsh.size == 1 ? type : hsh
      end

      def match_schema?(schema)
        return type_sym == schema.type_sym
        # TODO: eventually this could handle schema promotion for primitive schemas too
      end
    end

    class BytesSchema < PrimitiveSchema
      ERROR_INVALID_SCALE       = "Scale must be greater than or equal to 0"
      ERROR_INVALID_PRECISION   = "Precision must be positive"
      ERROR_PRECISION_TOO_SMALL = "Precision must be greater than scale"

      getter :precision, :scale
      @precision : Int64?
      @scale : Int64?

      def initialize(type : String, logical_type : String? = nil, precision : Int64? = nil, scale : Int64? = nil)
        super(type, logical_type)

        @precision = precision if precision
        @scale = scale if scale

        validate_decimal! if logical_type == DECIMAL_LOGICAL_TYPE
      end

      def to_avro(names = nil?)
        avro = super
        return avro if avro.is_a?(String)

        avro["precision"] = precision if precision
        avro["scale"] = scale if scale
        avro
      end

      def match_schema?(schema)
        return true if super

        if logical_type == DECIMAL_LOGICAL_TYPE && schema.logical_type == DECIMAL_LOGICAL_TYPE
          return precision == schema.precision && (scale || 0) == (schema.scale || 0)
        end

        false
      end

      private def validate_decimal!
        if !@precision.nil?
          raise Avro::SchemaParseError.new(ERROR_INVALID_PRECISION) unless @precision.as(Int64).positive?
        end

        if !@scale.nil?
          raise Avro::SchemaParseError.new(ERROR_INVALID_SCALE) if @scale.as(Int64).negative?
        end

        if !@precision.nil? && !@scale.nil?
          raise Avro::SchemaParseError.new(ERROR_PRECISION_TOO_SMALL) if @precision.as(Int64) < @scale.as(Int64)
        end
      end
    end

    class FixedSchema < NamedSchema
      getter :size, :precision, :scale

      def initialize(name : String, space : String?, size : Int64, names : Hash(String, Avro::Schema)? = nil, logical_type : String? = nil, aliases : Array(String)? = nil, precision : Int64? = nil, scale : Int64? = nil)
        # Ensure valid cto args
        unless size.is_a?(Int)
          raise AvroError.new("Fixed Schema requires a valid integer for size property.")
        end
        super("fixed", name, space, names, nil, logical_type, aliases)
        @size = size
        @precision = precision
        @scale = scale
      end

      def to_avro(names = Set.new)
        avro = super
        return avro if avro.is_a?(String)

        avro["size"] = size
        avro["precision"] = precision if precision
        avro["scale"] = scale if scale
        avro
      end

      def match_schema?(schema)
        return true if super && size == schema.size

        if logical_type == DECIMAL_LOGICAL_TYPE && schema.logical_type == DECIMAL_LOGICAL_TYPE
          return precision == schema.precision && (scale || 0) == (schema.scale || 0)
        end

        false
      end
    end

    class Field < Schema
      getter :type, :name, :default, :order, :doc, :aliases

      def initialize(type : String, name, default = :no_default, order = nil?, names = nil?, namespace = nil?, doc = nil?, aliases = nil?)
        super(type, nil)
        @type = subparse(type, names, namespace)
        @name = name
        @default = default
        @order = order
        @doc = doc
        @aliases = aliases
        @type_adapter = nil?
        validate_aliases! if aliases
        validate_default! if default? && !Avro.disable_field_default_validation
      end

      def default?
        @default != :no_default
      end

      def to_avro(names = Set.new)
        {"name" => name, "type" => type.to_avro(names)}.tap do |avro|
          avro["default"] = default if default?
          avro["order"] = order if order
          avro["doc"] = doc if doc
        end
      end

      def alias_names
        @alias_names ||= aliases.to_a
      end

      private def validate_default!
        type_for_default = if type.type_sym == :union
                             type.schemas.first
                           else
                             type
                           end
        case type_for_default.logical_type
        when DECIMAL_LOGICAL_TYPE
          # https://avro.apache.org/docs/1.11.1/specification/#schema-record
          # Default values for bytes and fixed fields are JSON strings, where Unicode code points 0-255 are mapped to unsigned 8-bit byte values 0-255
          options = SchemaValidator::DEFAULT_VALIDATION_OPTIONS.dup
          options[:encoded] = true
          Avro::SchemaValidator.validate!(type_for_default, default, options)
        else
          Avro::SchemaValidator.validate!(type_for_default, default)
        end
      rescue e : Avro::SchemaValidator::ValidationError
        raise Avro::SchemaParseError.new("Error validating default for #{name}: #{e.message}")
      end
    end
  end

  class SchemaParseError < Avro::AvroError
  end

  class UnknownSchemaError < SchemaParseError
    property type_name : String
    property default_namespace : String

    def initialize(type, default_namespace)
      @type_name = type
      @default_namespace = default_namespace
      super("#{type.inspect} is not a schema we know about.")
    end
  end

  module Name
    def self.extract_namespace(name : String, namespace : String?) : Tuple(String, String?)
      parts = name.split('.')
      if parts.size > 1
        namespace, name = parts[0..-2].join('.'), parts.last
      end
      return name, namespace
    end

    # Add a new schema object to the names dictionary (in place).
    def self.add_name(names, new_schema : Avro::Schema)
      new_fullname = new_schema.fullname
      if Avro::Schema::VALID_TYPES.includes?(new_fullname)
        raise SchemaParseError.new("#{new_fullname} is a reserved type name.")
      elsif names.nil?
        names = Hash(String, Avro::Schema).new
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
end
