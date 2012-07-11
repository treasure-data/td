require 'rubygems'
require 'rake'
require 'rake/testtask'
require 'rake/clean'

## build

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gemspec|
    gemspec.name = "td"
    gemspec.summary = "Treasure Data command line tool"
    gemspec.authors = ["Treasure Data, Inc."]
    gemspec.email   = "support@treasure-data.com"
    gemspec.homepage = "http://treasure-data.com/"
    gemspec.summary = "CLI to manage data on Treasure Data, the Hadoop-based cloud data warehousing"
    gemspec.description = "CLI to manage data on Treasure Data, the Hadoop-based cloud data warehousing"
    gemspec.has_rdoc = false
    gemspec.require_paths = ["lib"]
    gemspec.add_dependency "msgpack", "~> 0.4.4"
    gemspec.add_dependency "json", ">= 1.4.3"
    gemspec.add_dependency "hirb", ">= 0.4.5"
    gemspec.add_dependency "td-client", "~> 0.8.19"
    gemspec.add_dependency "td-logger", "~> 0.3.12"
    gemspec.add_development_dependency "rake", "~> 0.9"
    gemspec.add_development_dependency "jeweler", "~> 1.8"
    gemspec.test_files = Dir["test/**/*.rt"]
    gemspec.files = Dir["lib/**/*", "ext/**/*", "data/**/*", "test/**/*.rb", "test/**/*.rt"]
    gemspec.executables = ['td']
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler not available. Install it with: gem install jeweler"
end

VERSION_FILE = "lib/td/version.rb"

file VERSION_FILE => ["VERSION"] do |t|
  version = File.read("VERSION").strip
  File.open(VERSION_FILE, "w") {|f|
    f.write <<EOF
module TreasureData

VERSION = '#{version}'

end
EOF
  }
end

# workaround for >= 0 dependency
task :rm_gemspec do
  File.unlink "td.gemspec"
end

# workaround for >= 0 dependency
task :mv_gemfile do
  File.rename "Gemfile", "Gemfile.bak" rescue nil
end

# workaround for td >= 0 dependency
task :revert_gemfile do
  File.rename "Gemfile.bak", "Gemfile" rescue nil
end

task :default => [VERSION_FILE, :rm_gemspec, :mv_gemfile, :build, :revert_gemfile, :gemspec]

## dist

require 'erb'
require 'fileutils'
require 'tmpdir'

PROJECT_ROOT = File.expand_path("..", __FILE__)
$:.unshift "#{PROJECT_ROOT}/lib"

Dir[File.expand_path("../dist/**/*.rake", __FILE__)].each do |rake|
  import rake
end

def version
  require "td/version"
  TreasureData::VERSION
end

def distribution_files(type=nil)
  require "td/distribution"
  base_files = TreasureData::Distribution.files
  type_files = type ?
    Dir[File.expand_path("../dist/resources/#{type}/**/*", __FILE__)] :
    []
  #base_files.concat(type_files)
  base_files
end

### dir/path utils

def mkchdir(dir)
  FileUtils.mkdir_p(dir)
  Dir.chdir(dir) do |dir|
    yield(File.expand_path(dir))
  end
end

def pkg(filename)
  FileUtils.mkdir_p("pkg")
  File.expand_path("../pkg/#{filename}", __FILE__)
end

def tempdir
  Dir.mktmpdir do |dir|
    Dir.chdir(dir) do
      yield(dir)
    end
  end
end

def project_root
  File.dirname(__FILE__)
end

def resource(name)
  File.expand_path("../dist/resources/#{name}", __FILE__)
end

### assembles

def assemble_distribution(target_dir=Dir.pwd)
  distribution_files.each do |source|
    target = source.gsub(/^#{project_root}/, target_dir)
    FileUtils.mkdir_p(File.dirname(target))
    FileUtils.cp(source, target)
    puts target
  end
end

GEM_BLACKLIST = %w( bundler td )
def assemble_gems(target_dir=Dir.pwd)
  puts "installing gems locally: #{target_dir}/vendor/gems"
  FileUtils.mkdir_p "#{target_dir}/vendor/gems"
  #%x{ bundle install --path "#{target_dir}/vendor/gems" }
  system %{ bundle install --path "./vendor/gems" }
  raise "error running bundler (install)" unless $?.success?

  lines = %x{ bundle show }.strip.split("\n")
  raise "error running bundler (show)" unless $?.success?

  %x{ env BUNDLE_WITHOUT="development:test" bundle show }.split("\n").each do |line|
    if line =~ /^  \* (.*?) \((.*?)\)/
      next if GEM_BLACKLIST.include?($1)
      puts "vendoring: #{$1}-#{$2}"
      gem_dir = %x{ bundle show #{$1} }.strip
      FileUtils.mkdir_p "#{target_dir}/vendor/gems"
      %x{ cp -R "#{gem_dir}" "#{target_dir}/vendor/gems" }
    end
  end.compact
end

def assemble(source, target, perms=0644)
  FileUtils.mkdir_p(File.dirname(target))
  File.open(target, "w") do |f|
    f.puts ERB.new(File.read(source)).result(binding)
  end
  File.chmod(perms, target)
end
