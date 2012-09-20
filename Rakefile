require 'bundler'
Bundler::GemHelper.install_tasks

task :default => :build

# common methods for package build scripts
require 'fileutils'
require "erb"

def version
  require project_root_path('lib/td/version')
  TreasureData::VERSION
end

def project_root_path(path)
  "#{PROJECT_ROOT}/#{path}"
end

PROJECT_ROOT = File.expand_path(File.dirname(__FILE__))
USE_GEMS = ["#{PROJECT_ROOT}/pkg/td-#{version}.gem"]

def install_use_gems(target_dir)
  env = {
    'GEM_HOME' => target_dir,
    'GEM_PATH' => '',
  }
  USE_GEMS.each {|gem|
    system(env, "gem install #{gem}")
    raise "Command failed" unless $?.success?
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
  variables.each_pair {|k,v| ctx.define_singleton_method(k) { v } }
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
    FileUtils.rm_rf(dir) if success
  end
end

def download_resource(url)
  fname = File.basename(url).gsub(/\?.*$/,'')
  path = project_root_path("build/cache/#{fname}")
  if File.exists?(path) && Time.now - File.mtime < 24*60*60
    return path
  end
  FileUtils.mkdir_p File.dirname(path)
  sh "curl '#{url}' -o '#{path}'"
  path
end

Dir[File.expand_path("../dist/**/*.rake", __FILE__)].each do |rake|
  import rake
end

