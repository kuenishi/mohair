require 'mohair'
require 'riak'
require 'erb'

module Mohair

  MapperTemplate = <<EOMAP
function(v){
  var f = function(key, obj){
    var ret = {};
    <% select.each do |c| %>
    <%=  c %>
    <% end %>
    ret.__key = key;
    <%= where %>
  };
  var raw_obj = JSON.parse(v.values[0].data);
  if(raw_obj instanceof Array){
    var ret0 = [];
    for(var i in raw_obj){
      ret0 = ret0.concat(f(v.key, raw_obj[i]));
    }
    return ret0;
  }else{
    return f(v.key, raw_obj);
  }
}
EOMAP

  ReducerTemplate = <<EOREDUCE
function(values){
  var ret = {};
  // init lines
  <%= agg_init %>
  for(var i in values){
    var v=values[i];
    <%= agg_fun %>
  }
  return [ret];
}
EOREDUCE
  # if(!!(v.sum_age)){ ret.sum_age += v.sum_age; }

  GetAllMapper = <<GETALLMAPPER
function(v){
  var f = function(key, obj){
    var ret = obj;
    ret.__key = key;
    <%= where %>
  };
  var raw_obj = JSON.parse(v.values[0].data);
  if(raw_obj instanceof Array){
    var ret0 = [];
    for(var i in raw_obj){
      ret0 = ret0.concat(f(v.key, raw_obj[i]));
    }
    return ret0;
  }else{
    return f(v.key, raw_obj);
  }
}
GETALLMAPPER
  
  def Mohair.format_result results
    columns = Set.new
    results.each do |r|
      r.each do |k,v|
        columns.add(k)
      end
    end
    columns.delete('__key')
    cols = columns.to_a.join("\t| ")
    print "|               | #{cols}|\n"
    print "+---------------+------------------------------+\n"
    results.each do |r|
      print "| #{r['__key']}\t| "
      columns.to_a.each do |c|
        print "#{r[c]} \t| "
      end
      print "\n"
    end
  end

end
