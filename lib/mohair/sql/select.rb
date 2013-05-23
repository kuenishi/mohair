require 'mohair/selector'

module Mohair
    class Select
    def initialize tree
      @select = build_columns tree[:select]
      @from   = From.new tree[:from]
      @where  = Where.new tree[:where]
      @agg = false
    end

    def build_columns items
      reqs = []
      if items.class == Array then
        items.each do |i|
          reqs << maybe_column(i[:item])
        end
      elsif items.class == Hash then
        reqs << maybe_column(items[:item])
      elsif items.to_s == "*" then
        reqs = :all
      end
      reqs
    end

    def maybe_column item
      if item.class == Hash then
        if item[:function] then
          Function.new item
        else
          raise item
        end
      else
        Column.new item
      end
    end

    def bucket
      @from.bucket
    end

    def mapper
      where = @where.to_js
      if @select == :all then
        ERB.new(GetAlolMapper).result(binding)

      else
        select = []
        @select.each do |c| select << c.to_map_js end
        ERB.new(MapperTemplate).result(binding)
      end
    end

    def reducer
      @select.each do |c|
        if c.is_agg? then
          agg_init, agg_fun = c.to_reduce_js
          return ERB.new(ReducerTemplate).result(binding)
        end
      end
      return nil
    end
    
  end

  def any(arr, fun)
    arr.each do |e|
      if fun(e) then
        return true
      end
    end
    return false
  end

end
