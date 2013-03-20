require 'mohair'
require 'riak'

module Mohair
  class Selector
    def initialize argv
      @cols = []
      @from = []
      @where = []
      argv.shift
      parse_first argv
    end
    def parse_first tokens
      token = tokens.shift.downcase
      case token.downcase
      when 'from'
        parse_from tokens
      when 'where'
        parse_where tokens
      else
        @cols << token
        parse_first tokens
      end
    end
    def parse_from tokens
      token = tokens.shift.downcase
      p token
      case token.downcase
      when 'where'
        parse_where tokens
      else
        @from << token
      end
    end
    def parse_where tokens
    end
    def exec!
      pr
      @client = Riak::Client.new(:protocol => "http")
      @from.each do |b|
        bucket = @client.bucket(b)
        results = Riak::MapReduce.new(@client)
          .add(bucket)         ## keyfilsters and so on here
          .map("function(v){ return [v]; }", :keep => true) ## put logics here
          .run

        results.each do |o|
          print "{#{o["bucket"]}, #{o["key"]}} -> \n"
          o["values"].each do |data|
            print "\t#{o["values"][0]["data"]}\n"
          end
        end
        #          .map("function(v){ return [JSON.parse(v.values[0].data)]; }", :keep => true).run
      end
    end
    def pr
      print "query: <select> #{@cols} <from> #{@from};\n"
    end
  end
end
