# -*- coding: utf-8 -*-
require "fileutils"
require "shellwords"
require "zip/zip"

module TreasureData
  module Updater
    DEFAULT_TOOLBELT_URL = "http://toolbelt.treasuredata.com/"

    def self.raise_error(message)
      # TODO: Replace better Exception class
      raise RuntimeError.new(message)
    end

    # copied from TreasureData::Helpers to avoid load issue.
    def self.home_directory
      on_windows? ? ENV['USERPROFILE'].gsub("\\","/") : ENV['HOME']
    end

    def self.on_windows?
      RUBY_PLATFORM =~ /mswin32|mingw32/
    end

    def self.on_mac?
      RUBY_PLATFORM =~ /-darwin\d/
    end

    def self.updating_lock_path
      File.join(home_directory, ".td", "updating")
    end

    def self.installed_client_path
      File.expand_path("../../../../../..", __FILE__)
    end

    def self.updated_client_path
      File.join(home_directory, ".td", "updated")
    end

    def self.latest_local_version
      installed_version = client_version_from_path(installed_client_path)
      updated_version = client_version_from_path(updated_client_path)
      if compare_versions(updated_version, installed_version) > 0
        updated_version
      else
        installed_version
      end
    end

    def self.get_client_version_file(path)
      td_gems = Dir[File.join(path, "vendor/gems/td-*")]
      td_gems.each { |td_gem|
        if td_gem =~ /#{"#{Regexp.escape(path)}\/vendor\/gems\/td-\\d*.\\d*.\\d*"}/
          return File.join(td_gem, "/lib/td/version.rb")
        end
      }

      nil
    end

    def self.client_version_from_path(path)
      if version_file = get_client_version_file(path)
        File.read(version_file).match(/VERSION = '([^']+)'/)[1]
      else
        '0.0.0'
      end
    end

    def self.disable(message)
      @disable = message
    end

    def self.disable?
      !@disable.nil?
    end

    def self.disable_message
      @disable
    end

    def self.wait_for_lock(path, wait_for = 5, check_every = 0.5)
      start = Time.now.to_i
      while File.exists?(path)
        sleep check_every
        if (Time.now.to_i - start) > wait_for
          raise_error "Unable to acquire update lock"
        end
      end
      begin
        FileUtils.touch(path)
        ret = yield
      ensure
        FileUtils.rm_f(path)
      end
      ret
    end

    def self.package_category
      case
      when on_windows?
        'exe'
      when on_mac?
        'pkg'
      else
        raise_error "Non supported environment"
      end
    end

    def self.fetch(uri)
      require 'net/http'
      require 'openssl'

      # open-uri can't treat 'http -> https' redirection and
      # Net::HTTP.get_response can't get response from HTTPS endpoint.
      # So we use following code to avoid above issues.
      u = URI(uri)
      response = if u.scheme == 'https'
                   http = Net::HTTP.new(u.host, u.port)
                   http.use_ssl = true
                   http.verify_mode = OpenSSL::SSL::VERIFY_NONE
                   http.request(Net::HTTP::Get.new(u.path))
                 else
                   Net::HTTP.get_response(u)
                 end

      case response
      when Net::HTTPSuccess then response.body
      when Net::HTTPRedirection then fetch(response['Location'])
      else
        response.error!
      end
    end

    def self.endpoint_root
      ENV['TD_TOOLBELT_UPDATE_ROOT'] || DEFAULT_TOOLBELT_URL
    end
    #puts "endpoint_root: #{self.endpoint_root}"

    def self.version_endpoint
      "#{endpoint_root}/version.#{package_category}"
    end

    def self.update_package_endpoint
      "#{endpoint_root}/td-update-#{package_category}.zip"
    end

    def self.update(autoupdate = false)
      wait_for_lock(updating_lock_path, 5) do
        require "td"
        require 'open-uri'
        require "tmpdir"
        require "zip/zip"

        latest_version = fetch(version_endpoint)

        if compare_versions(latest_version, latest_local_version) > 0
          Dir.mktmpdir do |download_dir|
            File.open("#{download_dir}/td-update.zip", "wb") do |file|
              file.print fetch(update_package_endpoint)
            end

            Zip::ZipFile.open("#{download_dir}/td-update.zip") do |zip|
              zip.each do |entry|
                target = File.join(download_dir, entry.to_s)
                FileUtils.mkdir_p(File.dirname(target))
                zip.extract(entry, target) { true }
              end
            end

            FileUtils.rm "#{download_dir}/td-update.zip"

            old_version = latest_local_version
            new_version = client_version_from_path(download_dir)

            if compare_versions(new_version, old_version) < 0 && !autoupdate
              raise_error "Installed version (#{old_version}) is newer than the latest available update (#{new_version})"
            end

            FileUtils.rm_rf updated_client_path
            FileUtils.mkdir_p File.dirname(updated_client_path)
            FileUtils.cp_r(download_dir, updated_client_path)

            new_version
          end
        else
          false # already up to date
        end
      end
    ensure
      FileUtils.rm_f(updating_lock_path)
    end

    def self.compare_versions(first_version, second_version)
      first_version.split('.').map { |part| Integer(part) rescue part } <=> second_version.split('.').map { |part| Integer(part) rescue part }
    end

    def self.inject_libpath
      old_version = client_version_from_path(installed_client_path)
      new_version = client_version_from_path(updated_client_path)

      if compare_versions(new_version, old_version) > 0
        vendored_gems = Dir[File.join(updated_client_path, "vendor", "gems", "*")]
        vendored_gems.each do |vendored_gem|
          $:.unshift File.join(vendored_gem, "lib")
        end
        load('td/updater.rb') # reload updated updater
      end

      background_update!
    end

    def self.last_autoupdate_path
      File.join(home_directory, ".td", "autoupdate.last")
    end

    def self.background_update!
      if File.exists?(last_autoupdate_path)
        return if (Time.now.to_i - File.mtime(last_autoupdate_path).to_i) < 60 * 60 * 1 # every 1 hours
      end
      log_path = File.join(home_directory, '.td', 'autoupdate.log')
      FileUtils.mkdir_p File.dirname(log_path)
      td_binary = File.expand_path($0)
      pid = if defined?(RUBY_VERSION) and RUBY_VERSION =~ /^1\.8\.\d+/
        fork do
          exec("#{Shellwords.escape(td_binary)} update &> #{Shellwords.escape(log_path)} 2>&1")
        end
      else
        log_file = File.open(log_path, "w")
        spawn(td_binary, 'update', :err => log_file, :out => log_file)
      end
      Process.detach(pid)
      FileUtils.mkdir_p File.dirname(last_autoupdate_path)
      FileUtils.touch last_autoupdate_path
    end
  end
end
