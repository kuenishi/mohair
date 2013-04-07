require 'mohair'
require 'riak'
require 'erb'

module Mohair

  MapperTemplate = <<EOMAP
function(v){
  var obj = [JSON.parse(v.values[0].data)];
  var ret = {};
  <% select.each do |c| %>
  <%=  c %>
  <% end %>
  return ret;
}
EOMAP

  ReducerTemplate = <<EOREDUCE
function(v){
}
EOREDUCE

  
  class Column
    def initialize c
      if c.is_a? Hash then
        @type = :function
        @name = c[:function]
        @argv = c[:arguments]
      else
        @type = :column
        @name = c.to_s
      end
    end
    def line
      case @type
      when :function
        "ret[#{@name}] = #{@name}(obj[#{@argv[:item].to_s}]);"
      when :column
        "ret[#{@name}] = obj[#{@name}];"
      end
    end
  end

  class Selector
    def initialize tree
      @tree = tree
      @select = @tree[:select]
      @from   = @tree[:from][:name].to_s
      @where  = @tree[:where]
      p @select, @from, @where
      set_mapper_reducer!
    end

    def set_mapper_reducer!
      select = []
      @select.each do |i|
        c = Column.new i[:item]
        select << c.line
      end
      p select
      @mapper = ERB.new(MapperTemplate).result(binding)
      @reducer = nil
    end

    def set_mr mr

      imm = mr.map(mapper, :keep => true)
      if reducer then
        imm = imm.reduce(reducer, :keep => true)
      end
      imm
    end

    def exec!
      pr
      @client = Riak::Client.new(:protocol => "http")
      bucket = @client.bucket(@from)
      #   results = set_mr(Riak::MapReduce.new(@client)
      #                      .add(bucket)         ## keyfilsters and so on here
      #                    ).run

      #   results.each do |o|
      #     print "{#{o["bucket"]}, #{o["key"]}}\n"
      #     o["values"].each do |data|
      #       print "\t-> #{o["values"][0]["data"]}\n"
      #     end
      #   end
      #   #          .map("function(v){ return [JSON.parse(v.values[0].data)]; }", :keep => true).run
      # end
    end
    def pr
      print @mapper
      print @reducer
    end
  end
end
