
require "rubygems"
require "parslet"

module Mohair
  module Sql
    class Parser < Parslet::Parser
      rule(:integer)    { match('[0-9]').repeat(1) }

      rule(:space)      { match('\s').repeat(1) }
      rule(:space?)     { space.maybe }
      rule(:comma)      { str(',') >> space? }
      rule(:lparen)     { str('(') >> space? }
      rule(:rparen)     { str(')') >> space? }

      # logical operators
      rule(:eq)         { str('=') >> space? }
      rule(:neq)        { (str('!=') | str('<>') )>> space? }
      rule(:gt)         { str('>') >> space? }
      rule(:lt)         { str('<') >> space? }
      rule(:geq)        { str('>=') >> space? }
      rule(:leq)        { str('<=') >> space? }
      rule(:btw)        { str('between') >> space? }
      #rule(:like)       { str('like') >> space? }
      rule(:binop)      { eq | neq | gt | lt | geq | leq | btw }

      rule(:const)      { integer }
      rule(:term)       { const | item }

      rule(:bool_and)        { str('and') >> space? }
      rule(:bool_or)         { str('or') >> space? }


      rule(:identifier) { match('[a-z]').repeat(1) }

      rule(:function)   {
        identifier.as(:function) >> space? >>
        lparen >> arglist.as(:arguments) >> rparen
      }

      rule(:item)       { function | identifier }

      rule(:arglist)    {
        item.as(:item) >> (comma >> item.as(:item)).repeat
      }
      rule(:namelist)   {
        identifier.as(:name) >> (comma >> identifier.as(:name)).repeat
      }

      rule(:single_cond){
        term.as(:lhs) >> space? >> binop.as(:op) >> term.as(:rhs)
      }
      # rule(:combined_cond_and){
      #   condition.as(:lhs) >> bool_and >> condition.as(:rhs)
      # }
      # rule(:combined_cond_or){
      #   condition.as(:lhs) >> bool_or  >> condition.as(:rhs)
      # }
      rule(:condition)  {
        single_cond # | combined_cond_and | combined_cond_or
      }

      rule(:select_s)   {
        str('select').as(:op) >> space? >> (arglist | str('*')).as(:select)
      }
      rule(:from_s)     {
        str('from') >> space? >> namelist.as(:from)
      }
      rule(:where_s)    {
        str('where') >> space? >> condition.as(:where)
      }
      rule(:select)     {
        select_s >> space? >> from_s >> space? >> where_s.maybe
      }

      rule(:expression) { select } #| insert | create }
      root :expression
    end
  end
end
