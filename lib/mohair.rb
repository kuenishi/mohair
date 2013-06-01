require_relative './mohair/sql/parser'
require_relative './mohair/sql/tree'

require_relative './mohair/version'
require_relative './mohair/selector'
require_relative './mohair/inserter'

require 'json'
require 'optparse'
require 'logger'

LOG = Logger.new(STDERR)
LOG.level = Logger::INFO # WARN, INFO, DEBUG, ...

module Mohair
  
  def self.main

    q = nil #query!!
    index = nil
    host = 'localhost'
    port = 8098

    opt = OptionParser.new
    opt.banner = <<-EOS
usage:
 $ mohair -q "select foo, bar from bucket_name" [-i INDEX] [-s SERVER]
 $ mohair_dump <bucket_name> < sample_data.json

insert, delete sentence is future work
mohair version #{Mohair::VERSION}
    EOS
    opt.on('-h', '--help'){ puts opt; abort }
    opt.on('-d', '--debug'){
      LOG.level = Logger::DEBUG
    }
    opt.on('-v', '--version'){ puts opt; abort }
    opt.on('-q Q'){|v| q = v}
    opt.on('-i INDEX'){|v| index = v}
    opt.on('-s SERVER'){|v|
      host, port = v.split(':')
    }

    opt.parse!(ARGV)
    
    LOG.info("connecting #{host}:#{port}")

    parser = Sql::Parser.new
    sql_syntax_tree = parser.parse(q.strip)
    LOG.debug sql_syntax_tree

    case sql_syntax_tree[:op]
    when 'select'

      s = (Mohair.build sql_syntax_tree)

      LOG.debug "mapper->\n"
      LOG.debug s.mapper
      LOG.debug "reducer->\n"
      LOG.debug s.reducer
      
      client = Riak::Client.new(:protocol => "http",
                                :nodes => [{:host => host, :http_port => port}])
      bucket = Riak::MapReduce.new(client)
        .add(client.bucket(s.bucket))## keyfilsters and so on here
      
      reducer = s.reducer
      result = nil
      if reducer.nil? then
        result = bucket.map(s.mapper, :keep => true)
          .run
      else
        result = bucket.map(s.mapper, :keep => false)
          .reduce(reducer, :keep => true)
          .run
      end

      #LOG.debug "raw query result> #{result}"
      LOG.info "query result:"
      format_result result

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
