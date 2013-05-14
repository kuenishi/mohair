require "bundler/gem_tasks"
require 'bundler'  
Bundler::GemHelper.install_tasks

require 'rake/testtask'

Rake::TestTask.new(:test) do |t|
  t.libs << 'lib' << 'test'
  t.test_files = FileList['test/plugin/*.rb']
  t.verbose = true
end

task :default => :test
