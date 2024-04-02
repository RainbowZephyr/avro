require "./abstract_schema"

module Avro::Schemas
  class ArraySchema < AbstractSchema
    getter :items

    @items : (String | Avro::Schemas::AbstractSchema)

    def initialize(items : (String | JSON::Any), names : Hash(String, Avro::Schemas::AbstractSchema)? = nil, default_namespace : String? = nil)
      super("array")
      @items = subparse(items, names, default_namespace)
    end

    def to_avro(names = Set.new)
      super.merge({"items" => items.to_avro(names)})
    end
  end
end
