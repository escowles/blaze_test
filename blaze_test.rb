#!/usr/bin/env ruby

require 'faraday'
require 'benchmark'

# uris and namespaces
dc = 'http://purl.org/dc/elements/1.1/'
@pcdm = 'http://pcdm.org/models#'
base = "http://example.org/#{rand(0...99999)}"

collections_to_create = 50
members_to_create = 100000
members_per_collection = 10000

@blaze = 'http://localhost:9999/blazegraph/sparql'
@collection_ids = (0..collections_to_create).map {|i| "<#{base}/collection#{i}>" }
@object_ids = (0..members_to_create).map {|i| "<#{base}/object#{i}>" }

def insert(sparql)
  conn = Faraday.new(url: @blaze)
  conn.post do |post|
    post.headers['Content-Type'] = 'application/sparql-update'
    post.body = sparql
  end
end

def query(sparql)
  conn = Faraday.new(url: @blaze)
  conn.post do |post|
    post.headers['Content-Type'] = 'application/sparql-query'
    post.body = sparql
  end
end

create_collections = Benchmark.realtime do
  collection_triples = @collection_ids.map {|id| "#{id} a <#{@pcdm}Collection>; <#{dc}title> 'Collection' ." }
  resp = insert "insert data { #{collection_triples.join(" ")} }"
end
puts "Time to create #{collections_to_create} collections: #{create_collections.round(3)}s"

create_objects = Benchmark.realtime do
  object_triples = @object_ids.map {|id| "#{id} a <#{@pcdm}Object>; <#{dc}title> \"#{id.gsub(/.*\//, '')}\" . " }
  resp = insert "insert data { #{object_triples.join(" ")} }"
end
puts "Time to create #{members_to_create} objects: #{create_objects.round(3)}s"

associate_members = Benchmark.realtime do
  assoc_triples = @collection_ids.map {|id| "#{id} <#{@pcdm}hasMember> #{@object_ids.sample(members_per_collection).join(",")} ." }
  resp = insert "insert data { #{assoc_triples.join(" ")} }"
end
puts "Time to associate #{members_per_collection} members with each collections: #{associate_members.round(3)}s"

get_collection = Benchmark.realtime do
  col = @collection_ids.sample
  resp = query "select ?obj where { #{col} <#{@pcdm}hasMember> ?obj }"
  puts "objects in #{col}: #{resp.body.lines.select { |x| x.include? "<uri>" }.count}"
end
puts "Time to get collection: #{get_collection.round(3)}s"

update_collection_title = Benchmark.realtime do
  col = @collection_ids.sample
  resp = insert "delete { #{col} <#{dc}title> ?title } insert { #{col} <#{dc}title> 'Banana' } where { #{col} <#{dc}title> ?title }"
end
puts "Time to update collection title: #{update_collection_title.round(3)}s"

query_collections = Benchmark.realtime do
  obj = @object_ids.sample
  resp = query "select ?col where { ?col <#{@pcdm}hasMember> #{obj} }"
  puts "collections containing #{obj}: #{resp.body.lines.select { |x| x.include? "<uri>" }.count}"
end
puts "Time to list collections that collect a random object: #{query_collections.round(3)}s"
