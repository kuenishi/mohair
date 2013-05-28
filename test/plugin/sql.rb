require 'test_helper'
# require 'parslet'

class ParserTest < MiniTest::Unit::TestCase
  #include Mohair

  def setup
    @parser = Mohair::Sql::Parser.new
  end
  def teardown
  end
  def test_simples # check syntax parser with parslet
    [
     'select a from b',
     'select a, b from comme',
     'select b from far',
     'select c from d',
     'select a, c, d,e,f,f from b ',
     'select count(a) from d',
    ].each do |sql|
      s = @parser.parse sql
      assert_equal(expected = "select", actual = s[:op])
      assert(! s[:select].nil?)
      assert(! s[:from].nil?)
      assert(! (Mohair.build s).nil?)
    end
  end

  def test_bad_sql
    [
     'select',
     ' select a from b',
     'select a, b from c where a = "120.0, ~!@#$%^&*+_\}{\" asdfa"sf"',
     ].each do |bad_sql|
      assert_raises Parslet::ParseFailed do
        @parser.parse bad_sql
      end
    end
  end

  def test_where
    [
     'select a from b where a > 20',
     'select a,b from c where a = 20',
     'select a,b from c where 20 < a and b < 234',
     'select a,b from c where 20 < a or b = 234',
     'select a, b from c where a = "oo"',
     'select a, b from c where a = "oo" and c > 235',
     'select a, b from c where a = "120.0, ~!@#$%^&*+_\}{\" asdfasf"',
     #'select a from b where a > 20.0',
     #'select a from b where a > 0.0',
     #'select a from b where a > -0',
    ].each do |where_sql|
      s = @parser.parse where_sql
      assert_equal(expected = "select", actual = s[:op])
      assert(! s[:select].nil?)
      assert(! s[:from].nil?)
      assert(! s[:where].nil?)
      assert(! (Mohair.build s).nil?)
    end
  end

  def test_group_by
    [
     'select a from b group by c',
     'select a from b where a > 345 group by c',
     'select a from b where a > 345 and foo = "hoge" group by c',
    ].each do |where_sql|
      s = @parser.parse where_sql
      assert_equal(expected = "select", actual = s[:op])
      assert(! s[:select].nil?)
      assert(! s[:from].nil?)
      assert(! s[:group_by].nil?)
      assert(! (Mohair.build s).nil?)
    end
  end

end
