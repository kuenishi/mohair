## syntax tree
require 'mohair/sql/select'

module Mohair

  def Mohair.build tree
    case tree[:op]
    when 'select'
      Select.new tree
    when 'insert'
      Insert.new tree
    when 'update'
      Update.new tree
    when 'delete'
      Delete.new tree
    else
      LOG.error "bad :op", tree
    end
  end


  class Column
    def initialize item
      @name = item.to_s
    end

    def to_map_js
      "ret.#{@name} = obj.#{@name};"
    end

    def is_agg?
      false
    end
  end

  class Function
    def initialize item
      @name = item[:function].to_s
      @argv = []
      if item[:arguments] == Array then
        item[:arguments].each do |i|
          @argv << i[:item].to_s
        end
      else
        @argv << item[:arguments][:item]
      end
    end

    def to_map_js
      s = @argv[0]
      case @name
      when 'sum' then
        "ret.sum_#{s} = obj.#{s};"
      when 'avg' then
        "ret.sum_#{s} = obj.#{s};\n ret.count_#{s} = 1;"
      when 'count' then
        if s == 'key' then
          "if(!!(v.#{s})){ ret.count_#{s} = 1; }"
        else
          "if(!!(obj.#{s})){ ret.count_#{s} = 1; }"
        end
      end
    end

    def to_reduce_js

      s = @argv[0]
      case @name
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

    def is_agg?
      true
    end
  end

  class Insert
  end

  class Update
  end

  class Delete
  end

  class From
    def initialize tree
      @name = tree[:name]
    end
    def bucket
      @name.to_s
    end
  end

  class Condition
    def initialize tree
      if tree[:rhs].nil? && tree[:lhs].class == Hash then
        tree = tree[:lhs]
      end
      lhs = tree[:lhs]
      rhs = tree[:rhs]
      @op  = tree[:op].to_s

      @lhs = objectize lhs
      @rhs = objectize rhs
    end

    def objectize s
      if s.class == Hash then
        Condition.new s
      elsif (s.to_s =~ /^[0-9]+$/).nil? then
        s.to_s
      else
        s.to_i
      end
    end

    def jstify o
      if o.class == Fixnum then
        o
      elsif o.class == Condition then
        "( #{o.to_js} )"
      elsif (o[0] == "\"" and o[-1] == "\"") then
        o
      else
        "obj.#{o}"
      end
    end

    def to_js
      lhs = jstify @lhs
      rhs = jstify @rhs
      " (#{lhs}) #{operator2str(@op)} (#{rhs}) "
    end

    def operator2str op
      ## SQL to JS operator
      case op
      when '=' then '=='
      when '<>' then '!='
      when "and" then '&&'
      when 'or' then '||'
      else op
      end
    end

  end

  class Where
    def initialize tree
      if tree then
        @cond = Condition.new tree
      else
        @cond = nil
      end
    end
    def to_js
      if @cond.nil? then
        "return [ret];"
      else
        s = @cond.to_js
        "if(#{s}){ return [ret]; }else{ return[]; }"
      end
    end
  end

  class Order
  end

  class Group
    def initialize col
      @col = col.to_s
    end

    def col
      @col
    end
  end

end
