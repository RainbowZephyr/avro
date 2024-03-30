require "./avro"

SCHEMA = {
  "type"   => "record",
  "name"   => "User",
  "fields" => [
    {"name" => "name", "type" => "string"},
    {"name" => "id", "type" => "long"},
    {"name" => "city", "type" => "string"},
  ],
}.to_json

x = Avro::Schema.parse(SCHEMA)
puts x
