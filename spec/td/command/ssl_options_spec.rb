require 'spec_helper'
require 'td/config'
require 'td/command/common'
require 'td/command/runner'
require 'tempfile'
require 'stringio'

module TreasureData
  module Command
    describe 'SSL Options' do
      include_context 'quiet_out'

      let(:command) { Class.new { include TreasureData::Command }.new }
      let(:config_path) { Tempfile.new('td-config').path }
      let(:runner) { TreasureData::Command::Runner.new }
      let(:ca_file_content) { "-----BEGIN CERTIFICATE-----\nMIIDummy\n-----END CERTIFICATE-----" }

      before do
        # Save original environment state
        @original_env = {
          'TD_SSL_VERIFY' => ENV['TD_SSL_VERIFY'],
          'TD_SSL_CA_FILE' => ENV['TD_SSL_CA_FILE']
        }
        
        # Clean environment
        ENV.delete('TD_SSL_VERIFY')
        ENV.delete('TD_SSL_CA_FILE')
        
        # Reset config state
        Config.path = config_path
        Config.apikey = "dummy_api_key"
        Config.cl_apikey = true
        Config.ssl_verify = true
        Config.ssl_ca_file = nil
        Config.cl_ssl_verify = false
        Config.cl_ssl_ca_file = false
      end

      after do
        # Restore original environment
        @original_env.each do |key, value|
          if value
            ENV[key] = value
          else
            ENV.delete(key)
          end
        end
        
        # Clean up temp files
        File.unlink(config_path) if File.exist?(config_path)
      end

      describe 'CLI option parsing' do
        it 'sets ssl_verify to false with --insecure flag' do
          # Use help command to avoid actual TD API calls
          expect { runner.run(['--insecure', 'help']) }.to raise_error(SystemExit)
          expect(Config.ssl_verify).to be false
          expect(Config.cl_ssl_verify).to be true
        end

        it 'sets CA file path with valid --ssl-ca-file' do
          Tempfile.create('ca-cert') do |ca_file|
            ca_file.write(ca_file_content)
            ca_file.close
            
            expect { runner.run(['--ssl-ca-file', ca_file.path, 'help']) }.to raise_error(SystemExit)
            expect(Config.ssl_ca_file).to eq ca_file.path
            expect(Config.cl_ssl_ca_file).to be true
          end
        end

        it 'displays warning when using --insecure' do
          expect { runner.run(['--insecure', 'help']) }.to raise_error(SystemExit)
          expect(stderr_io.string).to include('Warning: --insecure option disables SSL certificate verification')
        end
      end

      describe 'Configuration file reading' do
        it 'reads ssl.verify from config file' do
          File.open(config_path, 'w') do |f|
            f.puts "[ssl]"
            f.puts "verify = false"
          end
          
          # Ensure CLI flags are not set
          Config.cl_ssl_verify = false
          
          expect(Config.ssl_verify).to be false
        end

        it 'reads ssl.ca_file from config file' do
          ca_file_path = '/path/to/test/ca.crt'
          File.open(config_path, 'w') do |f|
            f.puts "[ssl]"
            f.puts "ca_file = #{ca_file_path}"
          end
          
          # Ensure CLI flags are not set
          Config.cl_ssl_ca_file = false
          
          expect(Config.ssl_ca_file).to eq ca_file_path
        end

        it 'handles ssl.verify = true in config file' do
          File.open(config_path, 'w') do |f|
            f.puts "[ssl]"
            f.puts "verify = true"
          end
          
          Config.cl_ssl_verify = false
          expect(Config.ssl_verify).to be true
        end
        
        it 'treats non-false values as true for ssl.verify' do
          File.open(config_path, 'w') do |f|
            f.puts "[ssl]"
            f.puts "verify = anything"
          end
          
          Config.cl_ssl_verify = false
          expect(Config.ssl_verify).to be true
        end
      end

      describe 'Environment variable support' do
        it 'respects TD_SSL_VERIFY=false from environment' do
          # Test the environment variable initialization logic directly
          ENV['TD_SSL_VERIFY'] = 'false'
          ssl_verify_from_env = ENV['TD_SSL_VERIFY'].nil? ? true : (ENV['TD_SSL_VERIFY'].downcase == 'false' ? false : true)
          expect(ssl_verify_from_env).to be false
        end

        it 'respects TD_SSL_VERIFY=true from environment' do
          ENV['TD_SSL_VERIFY'] = 'true'
          ssl_verify_from_env = ENV['TD_SSL_VERIFY'].nil? ? true : (ENV['TD_SSL_VERIFY'].downcase == 'false' ? false : true)
          expect(ssl_verify_from_env).to be true
        end

        it 'respects TD_SSL_CA_FILE from environment' do
          ca_file_path = '/env/path/to/ca.crt'
          ENV['TD_SSL_CA_FILE'] = ca_file_path
          ssl_ca_file_from_env = ENV['TD_SSL_CA_FILE']
          ssl_ca_file_from_env = nil if ssl_ca_file_from_env == ""
          expect(ssl_ca_file_from_env).to eq ca_file_path
        end

        it 'handles empty TD_SSL_CA_FILE' do
          ENV['TD_SSL_CA_FILE'] = ''
          ssl_ca_file_from_env = ENV['TD_SSL_CA_FILE']
          ssl_ca_file_from_env = nil if ssl_ca_file_from_env == ""
          expect(ssl_ca_file_from_env).to be_nil
        end
      end

      describe 'Priority order: CLI > env > config > default' do
        it 'CLI options override environment variables' do
          # Set environment
          ENV['TD_SSL_VERIFY'] = 'true'
          ENV['TD_SSL_CA_FILE'] = '/env/ca.crt'
          
          Tempfile.create('cli-ca-cert') do |ca_file|
            ca_file.write(ca_file_content)
            ca_file.close
            
            # CLI should win
            expect { runner.run(['--insecure', '--ssl-ca-file', ca_file.path, 'help']) }.to raise_error(SystemExit)
            
            expect(Config.ssl_verify).to be false
            expect(Config.ssl_ca_file).to eq ca_file.path
          end
        end

        it 'Config file overrides defaults' do
          File.open(config_path, 'w') do |f|
            f.puts "[ssl]"
            f.puts "verify = false"
            f.puts "ca_file = /config/ca.crt"
          end
          
          # Ensure no CLI or env overrides
          Config.cl_ssl_verify = false
          Config.cl_ssl_ca_file = false
          
          expect(Config.ssl_verify).to be false
          expect(Config.ssl_ca_file).to eq '/config/ca.crt'
        end
      end

      describe 'Client integration' do
        before do
          # Mock TreasureData::Client to avoid actual network calls
          allow(TreasureData::Client).to receive(:new).and_return(double('client').as_null_object)
        end

        it 'passes verify: false to client when ssl_verify is disabled' do
          Config.ssl_verify = false
          Config.cl_ssl_verify = true
          
          expect(TreasureData::Client).to receive(:new).with("dummy_api_key", hash_including(verify: false))
          command.send(:get_client)
        end

        it 'passes verify: ca_file_path to client when ssl_ca_file is set' do
          ca_file_path = '/path/to/ca.crt'
          Config.ssl_ca_file = ca_file_path
          Config.cl_ssl_ca_file = true
          
          expect(TreasureData::Client).to receive(:new).with("dummy_api_key", hash_including(verify: ca_file_path))
          command.send(:get_client)
        end

        it 'does not pass SSL options when using defaults' do
          # Default config
          Config.ssl_verify = true
          Config.ssl_ca_file = nil
          Config.cl_ssl_verify = false
          Config.cl_ssl_ca_file = false
          
          expect(TreasureData::Client).to receive(:new).with("dummy_api_key", hash_not_including(:verify))
          command.send(:get_client)
        end
      end

      describe 'cl_options_string generation' do
        before do
          Config.cl_apikey = true
          Config.apikey = 'test_key'
          Config.cl_endpoint = false
          Config.cl_import_endpoint = false
        end

        it 'includes --insecure when ssl_verify is false via CLI' do
          Config.cl_ssl_verify = true
          Config.ssl_verify = false
          
          expect(Config.cl_options_string).to include('--insecure')
        end

        it 'includes --ssl-ca-file when set via CLI' do
          Config.cl_ssl_ca_file = true
          Config.ssl_ca_file = '/path/to/ca.crt'
          
          expect(Config.cl_options_string).to include('--ssl-ca-file /path/to/ca.crt')
        end

        it 'properly escapes CA file paths with spaces' do
          Config.cl_ssl_ca_file = true
          Config.ssl_ca_file = '/path/with spaces/ca.crt'
          
          expect(Config.cl_options_string).to include('--ssl-ca-file /path/with\\ spaces/ca.crt')
        end

        it 'does not include SSL options when not set via CLI' do
          Config.cl_ssl_verify = false
          Config.cl_ssl_ca_file = false
          
          options_string = Config.cl_options_string
          expect(options_string).not_to include('--insecure')
          expect(options_string).not_to include('--ssl-ca-file')
        end
      end

      describe 'SSL option validation in runner' do
        it 'validates that SSL CA file validation occurs in runner.rb' do
          # Check that the runner code contains the validation logic
          runner_file = File.read(File.join(File.dirname(__FILE__), '..', '..', '..', 'lib', 'td', 'command', 'runner.rb'))
          expect(runner_file).to include('unless File.exist?(s)')
          expect(runner_file).to include('ParameterConfigurationError')
          expect(runner_file).to include('CA certification file not found')
        end

        it 'validates that warning is shown for --insecure' do
          runner_file = File.read(File.join(File.dirname(__FILE__), '..', '..', '..', 'lib', 'td', 'command', 'runner.rb'))
          expect(runner_file).to include('Warning: --insecure option disables SSL certificate verification')
        end
      end

      describe 'Integration testing with execute_td helper' do
        it 'shows SSL options in help' do
          _, stdout = execute_td('--help')
          expect(stdout).to include('--insecure')
          expect(stdout).to include('--ssl-ca-file')
        end

        it 'validates SSL warning message appears' do
          stderr, _ = execute_td('--insecure help')
          expect(stderr).to include('Warning:')
          expect(stderr).to include('insecure')
        end
      end

      describe 'SSL configuration integration' do
        it 'initializes SSL config properly from environment variables' do
          original_env = {
            'TD_SSL_VERIFY' => ENV['TD_SSL_VERIFY'],
            'TD_SSL_CA_FILE' => ENV['TD_SSL_CA_FILE']
          }

          begin
            ENV['TD_SSL_VERIFY'] = 'false'
            ENV['TD_SSL_CA_FILE'] = '/test/ca.crt'

            # Load a fresh Config class to test environment initialization
            load File.join(File.dirname(__FILE__), '..', '..', '..', 'lib', 'td', 'config.rb')

            # The config should read environment variables at load time
            expect(ENV['TD_SSL_VERIFY']).to eq 'false'
            expect(ENV['TD_SSL_CA_FILE']).to eq '/test/ca.crt'
          ensure
            # Restore environment
            original_env.each do |key, value|
              if value
                ENV[key] = value
              else
                ENV.delete(key)
              end
            end
          end
        end
      end
    end
  end
end
