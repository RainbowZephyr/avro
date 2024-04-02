require "./abstract_schema"
require "./name"

module Avro::Schemas
  class NamedSchema < AbstractSchema
    getter :name, :namespace, :aliases

    def initialize(type : String, name : String, namespace : String? = nil, names : Hash(String, Avro::Schemas::AbstractSchema)? = nil, doc : String? = nil, logical_type : String? = nil, aliases : Array(String)? = nil)
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
end
