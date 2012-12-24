$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))

require 'rspec'
require 'json'

if ENV['SIMPLE_COV']
  # SimpleCov
  # https://github.com/colszowka/simplecov
  require 'simplecov'
  SimpleCov.start do 
    add_filter 'spec/'
    add_filter 'pkg/'
    add_filter 'vendor/'
  end
end
