require 'mohair'

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
    end
    def pr
      print "<select> #{@cols} <from> #{@from};\n"
    end
  end
end
