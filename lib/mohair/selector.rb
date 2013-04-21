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
  <%= where %>
}
EOMAP

  ReducerTemplate = <<EOREDUCE
function(values){
  var ret = {};
  // init lines
  ret.sum_age = 0;
  for(var i in values){
    var v=values[i];
    // agg lines
    if(!!(v.sum_age)){ ret.sum_age += v.sum_age; }
  }
  return [ret];
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
    def js_mapper
      s = @argv[:item].to_s
      case @name.to_s
      when 'sum' then
        "ret.sum_#{s} = obj.#{s};"
      when 'avg' then
        "ret.sum_#{s} = obj.#{s};\n ret.#{s}_count = 1;"
      when 'count' then
        "if(!!(obj.#{s})){ ret.count_#{s} = 1; }"
      end
    end
    def js_reducer
      s = @argv[:item].to_s
      case @name.to_s
      when 'sum' then
        ["ret.sum_#{s} = 0;", "if(!!(v.sum_#{s})){ ret.sum_#{s} += v.sum_#{s}; }"]
      when 'avg' then
        ["ret.sum_#{s} = 0; ret.count_#{s} = 0;",
         "if(!!(v.sum_#{s})){ ret.sum_#{s} += v.sum_#{s};\n ret.count_#{s} += v.count_#{s}; }"]
      when 'count' then
        ["ret.count_#{s} = 0;",
         "if(!!(v.count_#{s})){ ret.count_#{s} += v.count_#{s}; }"]
      end
    end

    def mapper_line
      case @type
      when :function
        js_mapper
      when :column
        "ret.#{@name} = obj.#{@name};"
      end
    end

    def agg? 
      case @name.to_s
      when 'sum' then
        true
      when 'avg' then
        true
      when 'count' then
        true
      else
        false
      end
    end
  end

  class Selector
    def initialize tree
      @tree = tree
      @select = @tree[:select]
      @from   = @tree[:from][:name].to_s
      @where  = @tree[:where]
      @agg = false
      # p @select, @from, @where
      set_mapper_reducer!
    end

    def set_mapper_reducer!
      select = []
      agg = []
      where = where2if
      @reducer = nil
      if @select == "*" then
        @mapper = ERB.new(GetAllMapper).result(binding)

      elsif not @select.is_a? Array then
        c = Column.new(@select[:item])
        select << c.mapper_line
        @mapper = ERB.new(MapperTemplate).result(binding)
        if c.agg? then
          @reducer = ERB.new(ReducerTemplate).result(binding)
        end
      else
        @select.each do |i|
          c = Column.new(i[:item])
          select << c.mapper_line
        end
        
        @mapper = ERB.new(MapperTemplate).result(binding)
        @reducer = ERB.new(ReducerTemplate).result(binding)
      end

      print "mapper:"
      print @mapper
      print "reducer:"
      print @reducer
      print "--\n"
    end

    def where2if
      if @where.nil? then
        return "return [ret];"
      else
        s = cond_str @where
        "if(#{s}){ return [ret]; }else{ return []; }"
      end
    end
    def operator2str op
      case op.to_s.chop
      when '=' then '=='
      when '<>' then '!='
      else op.to_s
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
      " (#{lhs}) #{operator2str(cond[:op])} (#{rhs}) "
    end

    def set_mr mr
      imm = mr.map(@mapper, :keep => @reducer.nil?)

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
      columns = Set.new
      @results.each do |r|
        r.each do |k,v|
          columns.add(k)
        end
      end
      columns.delete('__key')
      cols = columns.to_a.join("\t| ")
      print "|               | #{cols}|\n"
      print "+---------------+------------------------------+\n"
      @results.each do |r|
        print "| #{r['__key']}\t| "
        columns.to_a.each do |c|
          print "#{r[c]} \t| "
        end
        print "\n"
      end
      
    end
  end
end
