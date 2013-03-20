require "mohair/version"
require "mohair/selector"
require "mohair/inserter"

require "json"

module Mohair
  # Your code goes here...
  def self.main
    case ARGV[0].downcase
    when 'select'
      Selector.new(ARGV).exec!
    when 'insert'
    when 'show'
    when 'create'
    else
      print "bad query: ", ARGV, "\n"
    end
  end

  def self.do_dump
    objs = JSON.load(STDIN)
    Inserter.new(ARGV[0]).insert_all(objs)
  end
end
