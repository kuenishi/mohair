require "mohair/sql/parser"

require "mohair/version"
require "mohair/selector"
require "mohair/inserter"

require "json"

require "logger"
LOG = Logger.new(STDERR)
LOG.level = Logger::DEBUG # WARN, INFO, DEBUG, ...

module Mohair
  # Your code goes here...
  
  def self.main
    @parser = Sql::Parser.new
    sql_syntax_tree =  @parser.parse ARGV[0]

    case sql_syntax_tree[:op]
    when 'select'
      LOG.debug sql_syntax_tree

      conn = Selector.new(sql_syntax_tree)
      result = conn.exec!
      LOG.debug "raw query result> #{result}"
      LOG.info "query result:"
      conn.format_result
      
    # when :insert
    # when :show
    when :create
      LOG.error "CREATE sentence is unavailable at mohair"
    else
      LOG.error "bad query: ", result, "\n"
    end
  end

  def self.do_dump
    objs = JSON.load(STDIN)
    Inserter.new(ARGV[0]).insert_all(objs)
  end
end
