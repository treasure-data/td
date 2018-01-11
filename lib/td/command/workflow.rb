require 'td/helpers'
require 'td/updater'
require 'open3'
require 'pathname'
require 'time'
require 'yaml'

module TreasureData
  module Command
    include TreasureData::Helpers

    # The workflow entrypoint command. Invokes the digdag cli, passing on any command line arguments.
    def workflow(op, capture_output=false, check_prereqs=true)
      if Config.apikey.nil?
        raise ConfigError
      end
      check_digdag_cli if check_prereqs
      cmd = [
          java_cmd,
          '-Dio.digdag.cli.programName=td workflow',
          '-XX:+TieredCompilation', '-XX:TieredStopAtLevel=1', '-Xverify:none'
      ]

      FileUtils.mkdir_p digdag_tmp_dir
      Dir.mktmpdir(nil, digdag_tmp_dir) { |wd|
        env = {}
        digdag_config_path = File.join(wd, 'config')
        FileUtils.touch(digdag_config_path)
        workflow_endpoint = Config.workflow_endpoint

        # In the future passing config to digdag should use environment variables
        if Config.cl_apikey || workflow_endpoint != 'https://api-workflow.treasuredata.com'
          # If the user passes the apikey on the command line we cannot use the digdag td.conf plugin.
          # Instead, create a digdag configuration file with the endpoint and the specified apikey.
          apikey = TreasureData::Config.apikey
          env['TD_CONFIG_PATH'] = nil
          env['TREASURE_DATA_WORKFLOW_ENDPOINT'] = workflow_endpoint
          env['TD_API_KEY'] = apikey
          File.write(digdag_config_path, [
              "client.http.endpoint = #{workflow_endpoint}",
              "client.http.headers.authorization = TD1 #{apikey}",
              "secrets.td.apikey = #{apikey}"
          ].join($/) + $/)
          cmd << '-Dio.digdag.standards.td.secrets.enabled=false'
          cmd << "-Dconfig.td.default_endpoint=#{Config.endpoint_domain}"
        else
          # Use the digdag td.conf plugin to configure wf api and apikey.
          env['TREASURE_DATA_CONFIG_PATH'] = Config.path
          cmd << '-Dio.digdag.standards.td.client-configurator.enabled=true'
        end

        cmd << '-jar' << digdag_cli_path
        unless op.argv.empty?
          cmd << '--config' << digdag_config_path
        end
        cmd.concat(op.argv)

        unless ENV['TD_TOOLBELT_DEBUG'].nil?
          $stderr.puts cmd.to_s
        end

        if capture_output
          # TODO: use popen3 instead?
          stdout_str, stderr_str, status = Open3.capture3(env, *cmd)
          $stdout.write(stdout_str)
          $stderr.write(stderr_str)
          return status.exitstatus
        else
          Kernel::system(env, *cmd)
          return $?.exitstatus
        end
      }
    end

    def workflow_update(op)
      version = op.cmd_parse
      $stdout << "Downloading workflow module #{version}..."
      download_digdag(version)
      $stdout.puts ' Done.'
      return 0
    end

    # "Factory reset"
    def workflow_reset(op)
      op.cmd_parse # to show help

      $stdout << 'Removing workflow module...'
      FileUtils.rm_rf digdag_dir
      $stdout.puts ' Done.'
      return 0
    end

    def workflow_version(op)
      op.cmd_parse # to show help

      unless File.exists?(digdag_cli_path)
        $stderr.puts('Workflow module not yet installed.')
        return 1
      end

      $stdout.puts("Bundled Java: #{bundled_java?}")

      begin
        out, status = Open3.capture2e(java_cmd, '-version')
        raise unless status.success?
      rescue
        $stderr.puts('Failed to run java')
        return 1
      end
      $stdout.puts(out)

      version_op = List::CommandParser.new("workflow", [], [], nil, ['--version'], true)
      $stdout.write('Digdag version: ')
      workflow(version_op, capture_output=true, check_prereqs=false)
    end

    private
    def system_java_cmd
      if td_wf_java.nil? or td_wf_java.empty?
        'java'
      else
        td_wf_java
      end
    end

    private
    def bundled_java?
      if not td_wf_java.empty?
        return false
      end
      return Helpers.on_64bit_os?
    end

    private
    def td_wf_java
      ENV.fetch('TD_WF_JAVA', '').strip
    end

    def digdag_base_url(version=nil)
      base = 'http://toolbelt.treasure-data.com/digdag'
      if version.to_s == ''
        base
      else
        "#{base}-#{version}"
      end
    end

    private
    def digdag_url(version=nil)
      url = ENV.fetch('TD_DIGDAG_URL', '').strip
      return url unless url.empty?
      user = Config.read['account.user'] if File.exist?(Config.path)
      if user.nil? or user.strip.empty?
        return digdag_base_url
      end
      query = URI.encode_www_form('user' => user)
      "#{digdag_base_url(version)}?#{query}"
    end

    private
    def digdag_dir
      File.join(home_directory, '.td', 'digdag')
    end

    private
    def digdag_tmp_dir
      File.join(home_directory, '.td', 'digdag', 'tmp')
    end

    private
    def digdag_cli_path
      File.join(digdag_dir, 'digdag')
    end

    private
    def digdag_jre_dir
      File.join(digdag_dir, 'jre')
    end

    private
    def digdag_jre_tmp_dir
      File.join(digdag_dir, 'jre.tmp')
    end

    private
    def java_cmd
      if bundled_java?
        digdag_java_path
      else
        system_java_cmd
      end
    end

    private
    def digdag_java_path
      File.join(digdag_jre_dir, 'bin', 'java')
    end

    private
    def digdag_cli_tmp_path
      File.join(digdag_dir, 'digdag.tmp')
    end

    private
    def jre_archive
      # XXX (dano): platform detection could be more robust
      if Helpers.on_64bit_os?
        if Helpers.on_windows?
          return 'win_x64'
        elsif Helpers.on_mac?
          return 'mac_x64'
        else # Assume linux
          return 'lin_x64'
        end
      end
      raise 'OS architecture not supported'
    end

    private
    def jre_url
      base_url = ENV.fetch('TD_WF_JRE_BASE_URL', 'http://toolbelt.treasuredata.com/digdag/jdk/')
      "#{base_url}#{jre_archive}"
    end

    private
    def fail_system_java
      raise WorkflowError, <<EOF
A suitable installed version of Java could not be found and and Java cannot be
automatically installed for this OS.

Please install at least Java 8u71.
EOF
    end

    private
    def detect_system_java
      begin
        output, status = Open3.capture2e(system_java_cmd, '-version')
      rescue => e
        return false
      end
      unless status.success?
        return false
      end
      if output =~ /openjdk version/ or output =~ /java version/
        m = output.match(/version "(\d+)\.(\d+)\.(\d+)(?:_(\d+))"/)
        if not m or m.size < 4
          return false
        end
        # Check for at least Java 8. Let digdag itself verify revision.
        major = m[1].to_i
        minor = m[2].to_i
        if major < 1 or minor < 8
          return false
        end
      end
      return true
    end

    private
    def check_system_java
      # Trust the user if they've specified a jre to use
      if td_wf_java.empty?
        unless detect_system_java
          fail_system_java
        end
      end
    end

    # Follow all redirects and return the resulting url
    def resolve_url(url)
      require 'net/http'
      require 'openssl'

      uri = URI(url)
      http_class = Command.get_http_class
      http = http_class.new(uri.host, uri.port)

      if uri.scheme == 'https'
        http.use_ssl = true
        http.verify_mode = OpenSSL::SSL::VERIFY_NONE
      end

      http.request_get(uri.path + (uri.query ? '?' + uri.query : '')) {|response|
        if response.class == Net::HTTPOK
          return url
        elsif response.is_a?(Net::HTTPRedirection)
          unless ENV['TD_TOOLBELT_DEBUG'].nil?
            $stdout.puts "redirect '#{url}' to '#{response['Location']}'... "
          end
          return resolve_url(response['Location'])
        else
          raise Command::UpdateError,
            "An error occurred when fetching from '#{uri}' " \
            "(#{response.class.to_s}: #{response.message})."
          return false
        end
      }
    end

    private
    def download_java
      if File.exists?(digdag_jre_dir)
        return
      end

      require 'net/http'
      require 'openssl'

      Dir.mktmpdir do |download_dir|
        indicator = Command::TimeBasedDownloadProgressIndicator.new(
            'Downloading Java...', Time.new.to_i, 2)
        status = nil
        real_jre_uri = URI(resolve_url(jre_url))
        jre_filename = Pathname.new(real_jre_uri.path).basename.to_s
        download_path = File.join(download_dir, jre_filename)
        File.open(download_path, 'wb') do |file|
          status = Updater.stream_fetch(jre_url, file) {
            indicator.update
          }
        end
        indicator.finish

        $stdout.puts

        unless status
          raise WorkflowError, 'Failed to download Java.'
        end

        $stdout.print 'Installing Java... '
        FileUtils.rm_rf digdag_jre_tmp_dir
        FileUtils.mkdir_p digdag_jre_tmp_dir
        extract_archive(download_path, digdag_jre_tmp_dir, 1)
        FileUtils.mv digdag_jre_tmp_dir, digdag_jre_dir
        $stdout.puts 'done'
      end
    end

    private
    def extract_archive(archive, destination, strip)
      if archive.end_with? '.tar.gz'
        extract_tarball(archive, destination, strip)
      elsif archive.end_with? '.zip'
        extract_zip(archive, destination, strip)
      end
    end

    private
    def extract_zip(zip_archive, destination, strip)
      require 'fileutils'
      require 'zip/zip'
      Zip::ZipFile.open(zip_archive) { |zip_file|
        zip_file.each { |f|
          stripped = strip_components(f.name, strip)
          if stripped.empty?
            next
          end
          dest = File.join destination, stripped
          FileUtils.rm_rf dest if File.exist? dest
          FileUtils.mkdir_p(File.dirname(dest))
          zip_file.extract(f, dest)
        }
      }
    end

    # http://stackoverflow.com/a/31310593
    TAR_LONGLINK = '././@LongLink'
    private
    def extract_tarball(tar_gz_archive, destination, strip)
      require 'fileutils'
      require 'rubygems/package'
      require 'zlib'

      Zlib::GzipReader.open tar_gz_archive do |gzip_reader|
        Gem::Package::TarReader.new(gzip_reader) do |tar|
          filename = nil
          tar.each do |entry|
            # Handle LongLink
            if entry.full_name == TAR_LONGLINK
              filename = entry.read.strip
              next
            end
            filename ||= entry.full_name

            # Strip path components
            stripped = strip_components(filename, strip)
            filename = nil
            if stripped.empty?
              next
            end
            dest = File.join destination, stripped

            if entry.directory? || (entry.header.typeflag == '' && entry.full_name.end_with?('/'))
              File.rm_rf dest if File.file? dest
              FileUtils.mkdir_p dest, :mode => entry.header.mode, :verbose => false
            elsif entry.file? || (entry.header.typeflag == '' && !entry.full_name.end_with?('/'))
              FileUtils.rm_rf dest if File.exist? dest
              FileUtils.mkdir_p File.dirname dest
              File.open dest, "wb" do |f|
                f.print entry.read
              end
              FileUtils.chmod entry.header.mode, dest, :verbose => false
            elsif entry.header.typeflag == '2' #Symlink!
              File.symlink entry.header.linkname, dest
            else
              raise "Unkown tar entry: #{entry.full_name} type: #{entry.header.typeflag}."
            end
          end
        end
      end
    end

    def strip_components(filename, strip)
      File.join(Pathname.new(filename).each_filename.drop(strip))
    end

    private
    def check_digdag_cli
      check_system_java unless bundled_java?

      unless File.exists?(digdag_cli_path)
        $stderr.puts 'Workflow module not yet installed, download now? [Y/n]'
        line = $stdin.gets
        line.strip!
        if (not line.empty?) and (line !~ /^y(?:es)?$/i)
          raise WorkflowError, 'Aborted'
        end
        download_digdag
      end
    end

    def download_digdag(version=nil)
      require 'net/http'
      require 'openssl'

      FileUtils.mkdir_p digdag_dir

      if bundled_java?
        download_java
      end

      Dir.mktmpdir do |download_dir|
        indicator = Command::TimeBasedDownloadProgressIndicator.new(
            'Downloading workflow module...', Time.new.to_i, 2)
        status = nil
        download_path = File.join(download_dir, 'digdag')
        File.open(download_path, 'wb') do |file|
          status = Updater.stream_fetch(digdag_url(version), file) {
            indicator.update
          }
        end
        indicator.finish

        $stdout.puts

        unless status
          raise WorkflowError, 'Failed to download workflow module.'
        end

        $stdout.print 'Installing workflow module... '
        FileUtils.rm_rf(digdag_cli_tmp_path)
        FileUtils.cp(download_path, digdag_cli_tmp_path)
        FileUtils.chmod('a=xr', digdag_cli_tmp_path)
        FileUtils.mv(digdag_cli_tmp_path, digdag_cli_path)
        $stdout.puts 'done'
      end
    end
  end
end
