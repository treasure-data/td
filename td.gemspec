# encoding: utf-8
$:.push File.expand_path('../lib', __FILE__)
require 'td/version'

Gem::Specification.new do |gem|
  gem.name                  = "td"
  gem.description           = "CLI to manage data on Treasure Data, the Hadoop-based cloud data warehousing"
  gem.homepage              = "http://treasure-data.com/"
  gem.summary               = "CLI to manage data on Treasure Data, the Hadoop-based cloud data warehousing"
  gem.version               = TreasureData::TOOLBELT_VERSION
  gem.authors               = ["Treasure Data, Inc."]
  gem.email                 = "support@treasure-data.com"
  gem.has_rdoc              = false
  gem.files                 = `git ls-files`.split("\n").select { |f| !f.start_with?('dist') }
  gem.test_files            = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables           = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.require_paths         = ['lib']
  gem.required_ruby_version = '>= 1.9'

  gem.add_dependency "msgpack", [">= 0.4.4", "!= 0.5.0", "!= 0.5.1", "!= 0.5.2", "!= 0.5.3", "< 0.5.12"]
  gem.add_dependency "yajl-ruby", "~> 1.1"
  gem.add_dependency "hirb", ">= 0.4.5"
  gem.add_dependency "parallel", "~> 0.6.1"
  gem.add_dependency "td-client", "~> 0.8.74"
  gem.add_dependency "td-logger", "~> 0.3.21"
  gem.add_dependency "rubyzip", "~> 1.1.7"
  gem.add_dependency "zip-zip", "~> 0.3"
  gem.add_development_dependency "rake", "~> 0.9"
  gem.add_development_dependency "rspec", "~> 2.11.0"
  gem.add_development_dependency "simplecov", "~> 0.10.0"
  gem.add_development_dependency 'coveralls'
end
