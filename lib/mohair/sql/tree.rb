## syntax tree

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

  class Select
    def initialize tree
      @select = tree[:select]
      @from   = From.new tree[:from]
      if tree[:where] then
        @where  = Where.new tree[:where]
      end        
      @agg = false
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
  end

  class Condition
    def initialize tree
      if tree[:rhs].nil? && tree[:lhs].class == Hash then
        tree = tree[:lhs]
      end
      @lhs = tree[:lhs].to_s
      @rhs = tree[:rhs].to_s
      @op  = tree[:op].to_s

      if (@lhs =~ /^[0-9]+$/).nil? then
      else
        @lhs = @lhs.to_i
      end

      if (@rhs =~ /^[0-9]+$/).nil? then
      else
        @rhs = @rhs.to_i
      end

    end
  end

  class Where
    def initialize tree
      @cond = Condition.new tree
    end
  end

  class Order
  end

  class Group
  end

end
