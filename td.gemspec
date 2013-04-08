# encoding: utf-8
$:.push File.expand_path('../lib', __FILE__)
require 'td/version'

Gem::Specification.new do |gem|
  gem.name        = "td"
  gem.description = "CLI to manage data on Treasure Data, the Hadoop-based cloud data warehousing"
  gem.homepage    = "http://treasure-data.com/"
  gem.summary     = "CLI to manage data on Treasure Data, the Hadoop-based cloud data warehousing"
  gem.version     = TreasureData::VERSION
  gem.authors     = ["Treasure Data, Inc."]
  gem.email       = "support@treasure-data.com"
  gem.has_rdoc    = false
  gem.files       = `git ls-files`.split("\n")
  gem.test_files  = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.require_paths = ['lib']

  gem.add_dependency "msgpack", "~> 0.4.4"
  gem.add_dependency "yajl-ruby", "~> 1.1.0"
  gem.add_dependency "hirb", ">= 0.4.5"
  gem.add_dependency "parallel", "~> 0.5.19"
  gem.add_dependency "td-client", "~> 0.8.46"
  gem.add_dependency "td-logger", "~> 0.3.16"
  gem.add_development_dependency "rake", "~> 0.9"
  gem.add_development_dependency "rspec", "~> 2.11.0"
  gem.add_development_dependency "simplecov", "~> 0.5.4"
end
