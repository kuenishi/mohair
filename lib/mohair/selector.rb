require 'mohair'
require 'riak'
require 'erb'

module Mohair

  MapperTemplate = <<EOMAP
function(v){
  var obj = JSON.parse(v.values[0].data);
  var ret = {};
  <% select.each do |c| %>
  <%=  c %>
  <% end %>
  ret.__key = v.key;
  //return [ret];
  <%= where %>
}
EOMAP

  ReducerTemplate = <<EOREDUCE
function(v){
}
EOREDUCE

  GetAllMapper = <<GETALLMAPPER
function(v){
  var ret = JSON.parse(v.values[0].data);
  ret.__key = v.key;
  <%= where %>
}
GETALLMAPPER
  
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
        "ret.#{@name} = obj.#{@name};"
      end
    end
  end

  class Selector
    def initialize tree
      @tree = tree
      @select = @tree[:select]
      @from   = @tree[:from][:name].to_s
      @where  = @tree[:where]
      # p @select, @from, @where
      set_mapper_reducer!
    end

    def set_mapper_reducer!
      select = []
      where = where2if
      if @select == "*" then
        @mapper = GetAllMapper
      elsif not @select.is_a? Array then
        select << Column.new(@select[:item]).line
        @mapper = ERB.new(MapperTemplate).result(binding)
      else
        @select.each do |i|
          c = Column.new(i[:item])
          select << c.line
        end
        @mapper = ERB.new(MapperTemplate).result(binding)
      end
      print @mapper
      @reducer = nil
    end

    def where2if
      if @where.nil? then
        return "return [ret];"
      else
        s = cond_str @where
        "if(#{s}){ return [ret]; }else{ return []; }"
      end
    end
    def cond_str cond
      lhs = cond[:lhs]
      rhs = cond[:rhs]
      if (lhs.to_s =~ /^[0-9]+$/).nil? then
        lhs = "obj.#{lhs}"
      else
        lhs = lhs.to_i
      end
      if (rhs.to_s =~ /[0-9]+$/).nil? then
        rhs = "obj.#{rhs}"
      else
        rhs = rhs.to_i
      end
      " (#{lhs}) #{cond[:op]} (#{rhs}) "
    end

    def set_mr mr
      imm = mr.map(@mapper, :keep => true)

      if @reducer then
        imm = imm.reduce(@reducer, :keep => true)
      end
      imm
    end

    def exec!
      @client = Riak::Client.new(:protocol => "http")
      bucket = @client.bucket(@from)
      @results = set_mr(Riak::MapReduce.new(@client)
                         .add(bucket)         ## keyfilsters and so on here
                       ).run
    end
    def pr
      print @mapper
      print @reducer
    end

    def format_result
      columns = []
      #@results.each do |r|
      #end
      p @results
    end
  end
end
