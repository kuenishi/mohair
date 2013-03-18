require "mohair/version"
require "mohair/selector"

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
end
