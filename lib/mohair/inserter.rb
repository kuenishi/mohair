require 'mohair'
require 'riak'

module Mohair
  class Inserter
    def initialize bucket_name
      @bucket_name = bucket_name
    end
    def insert_all objs
      @client = Riak::Client.new(:protocol => "http",
                                :nodes => [
                                           {:http_port => 10018}])
      bucket = @client.bucket(@bucket_name)
      if bucket.props["allow_mult"] then
        $stderr.puts "allow_mult should be false (how to handle siblilngs?)"
        return
      end

      objs.each do |obj|
        r_o = Riak::RObject.new(bucket, obj["key"])

        data = r_o.data = obj["data"]
        obj["data"].each do |k,v|
          begin
            if integer? v then r_o.indexes[k + "_int"] << v end
          rescue
            r_o.indexes[k + "_bin"] << v
          end
        end
        r_o.content_type = 'application/json'
        r_o.store
        print obj["key"] , "\t => ", data, "\n"
      end
    end
    
    def exec!
    end
    def pr
    end
  end
end
