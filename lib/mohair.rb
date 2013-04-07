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
    result =  @parser.parse ARGV[0]

    p result
    
    case result[:op]
    when 'select'
      LOG.debug result
      Selector.new(result).exec!
    # when :insert
    # when :show
    # when :create
    #   LOG.error "CREATE sentence is unavailable at mohair"
    else
      LOG.error "bad query: ", result, "\n"
    end
  end

  def self.do_dump
    objs = JSON.load(STDIN)
    Inserter.new(ARGV[0]).insert_all(objs)
  end
end
