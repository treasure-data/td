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
  gem.files                 = `git ls-files`.split("\n").select { |f| !f.start_with?('dist') }
  gem.test_files            = `git ls-files -- {test,spec,features}/*`.split("\n")
  gem.executables           = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.require_paths         = ['lib']
  gem.required_ruby_version = '>= 2.1'

  gem.add_dependency "msgpack"
  gem.add_dependency "yajl-ruby", "~> 1.1"
  gem.add_dependency "hirb", ">= 0.4.5"
  gem.add_dependency "parallel", "~> 1.8"
  gem.add_dependency "td-client", ">= 1.0.6", "< 2"
  gem.add_dependency "td-logger", ">= 0.3.21", "< 2"
  gem.add_dependency "rubyzip", ">= 1.2.1"
  gem.add_dependency "zip-zip", "~> 0.3"
  gem.add_dependency "ruby-progressbar", "~> 1.7"
  gem.add_development_dependency "rake"
  gem.add_development_dependency "rspec"
  gem.add_development_dependency "simplecov"
  gem.add_development_dependency 'coveralls'
end
