require "bundler/gem_tasks"
require 'rake/testtask'

Rake::Task[:release].clear

Rake::TestTask.new(:test) do |t|
  t.libs << 'lib' << 'test'
  t.pattern = 'test/**/test_*.rb'
end

task default: :test
