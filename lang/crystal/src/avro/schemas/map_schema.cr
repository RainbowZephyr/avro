require "./abstract_schema"

module Avro::Schemas 
  class MapSchema < AbstractSchema
    getter :values
    @values : Avro::Schemas::AbstractSchema

    def initialize(values, names : Hash(String, Avro::Schemas::AbstractSchema)? = nil, default_namespace : String? = nil)
      super("map")
      @values = subparse(values, names, default_namespace)
    end

    def to_avro(names = Set.new)
      super.merge({"values" => values.to_avro(names)})
    end
  end
end
