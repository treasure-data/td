require 'rubygems'
require 'bundler'
require 'zip/zip'
Bundler::GemHelper.install_tasks

task :default => :build

# common methods for package build scripts
require 'fileutils'
require "erb"

def version
  require project_root_path('lib/td/version')
  TreasureData::TOOLBELT_VERSION
end

def project_root_path(path)
  "#{PROJECT_ROOT}/#{path}"
end

PROJECT_ROOT = File.expand_path(File.dirname(__FILE__))
USE_GEMS = ["#{PROJECT_ROOT}/pkg/td-#{version}.gem"]

def install_use_gems(target_dir)
  unless ENV['GEM_HOME'].to_s.empty?
    puts "**"
    puts "** WARNING"
    puts "**"
    puts "** GEM_HOME is already set. Created package might be broken."
    puts "** RVM surely breaks the package. Use rbenv instead."
    puts "**"
  end

  require 'rubygems/gem_runner'
  require 'rubygems/rdoc' # avoid `Gem.finish_resolve` raises error

  # system(env, cmd) doesn't work with ruby 1.8
  ENV['GEM_HOME'] = target_dir
  ENV['GEM_PATH'] = ''
  USE_GEMS.each {|gem|
    begin
      # this is a hack to have the dependency handling for the 'td' gem
      #   pick up a local gem for 'td-client' so as to be able to build
      #   and test the 'toolbelt' package without publishing the 'td-client'
      #   gem on rubygems.com
      unless ENV['TD_TOOLBELT_LOCAL_CLIENT_GEM'].nil?
        unless File.exists? ENV['TD_TOOLBELT_LOCAL_CLIENT_GEM']
          raise "Cannot find gem file with path #{ENV['TD_TOOLBELT_LOCAL_CLIENT_GEM']}"
        end
        puts "Copy local gem #{ENV['TD_TOOLBELT_LOCAL_CLIENT_GEM']} to #{Dir.pwd}"
        FileUtils.cp File.expand_path(ENV['TD_TOOLBELT_LOCAL_CLIENT_GEM']), Dir.pwd
      end
      Gem::GemRunner.new.run ["install", gem, "--no-rdoc", "--no-ri"]
    rescue Gem::SystemExitException => e
      unless e.exit_code.zero?
        raise e
      end
    end
  }
  FileUtils.mv Dir.glob("#{target_dir}/gems/*"), target_dir
  FileUtils.rm_f Dir.glob("#{target_dir}/*.gem")
  %W(bin cache doc gems specifications build_info).each { |dir|
    FileUtils.remove_dir("#{target_dir}/#{dir}", true)
  }
end

def resource_path(path)
  project_root_path("dist/resources/#{path}")
end

def install_resource(resource_name, target_path, mode)
  FileUtils.mkdir_p File.dirname(target_path)
  FileUtils.cp resource_path(resource_name), target_path
  File.chmod(mode, target_path)
end

def install_erb_resource(resource_name, target_path, mode, variables)
  FileUtils.mkdir_p File.dirname(target_path)
  erb_raw = File.read resource_path(resource_name)

  ctx = Object.new
  variables.each_pair {|k,v|
    # ctx.define_singleton_method(k) { v } doesn't work with ruby 1.8
    (class<<ctx;self;end).send(:define_method, k) { v }
  }
  data = ERB.new(erb_raw).result(ctx.instance_eval("binding"))

  File.open(target_path, "w") do |f|
    f.write data
  end
  File.chmod(mode, target_path)
end

def mkchdir(dir, &block)
  FileUtils.mkdir_p dir
  Dir.chdir(dir) do |dir|
    yield File.expand_path(dir)
  end
end

def build_dir_path(type)
  project_root_path("build/#{type}.build")
end

def create_build_dir(type, &block)
  dir = build_dir_path(type)
  FileUtils.rm_rf dir
  FileUtils.mkdir_p dir
  begin
    mkchdir(dir, &block)
    success = true
  ensure
    #FileUtils.rm_rf(dir) if success
  end
end

def download_resource(url)
  fname = File.basename(url).gsub(/\?.*$/,'')
  path = project_root_path("build/cache/#{fname}")
  if File.exists?(path) && Time.now - File.mtime(path) < 24*60*60
    return path
  end
  FileUtils.mkdir_p File.dirname(path)
  begin
    sh "curl '#{url}' -o '#{path}'"
  rescue
    sh "wget '#{url}' -O '#{path}'"
  end
  path
end

def zip_files(zip_name, target_dir)
  Zip::ZipFile.open(zip_name, Zip::ZipFile::CREATE) do |zip|
    Dir["#{target_dir}/**/*"].each do |file|
      zipped_path = file[target_dir.length + 1..-1]
      zip.add(zipped_path, file) { true }
    end
  end
end

Dir[File.expand_path("../dist/**/*.rake", __FILE__)].each do |rake|
  import rake
end

require 'rspec/core'
require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec) do |spec|
  spec.pattern = FileList['spec/**/*_spec.rb']
end

task :coverage do |t|
  ENV['SIMPLE_COV'] = '1'
  Rake::Task["spec"].invoke
end
