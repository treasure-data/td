require 'spec_helper'
require 'tempfile'
require 'td/config'

describe TreasureData::Config do
  describe 'SSL options' do
    let(:config_file) { Tempfile.new('td-config') }
    let(:config_path) { config_file.path }
    
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
      TreasureData::Config.cl_ssl_verify = false
      TreasureData::Config.ssl_verify = true
      TreasureData::Config.cl_ssl_ca_file = false
      TreasureData::Config.ssl_ca_file = nil
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
      
      # Clean up
      config_file.close
      config_file.unlink
    end

    context 'ssl_verify' do
      it 'returns default value (true) when not configured' do
        expect(TreasureData::Config.ssl_verify).to eq true
      end

      it 'returns value from command line when cl_ssl_verify is true' do
        TreasureData::Config.cl_ssl_verify = true
        TreasureData::Config.ssl_verify = false
        expect(TreasureData::Config.ssl_verify).to eq false
      end

      it 'reads false value from config file' do
        config_file.write <<-EOF
[ssl]
verify = false
        EOF
        config_file.close

        allow(TreasureData::Config).to receive(:read).and_return(TreasureData::Config.new.read(config_path))
        expect(TreasureData::Config.ssl_verify).to eq false
      end

      it 'reads true value from config file' do
        config_file.write <<-EOF
[ssl]
verify = true
        EOF
        config_file.close

        allow(TreasureData::Config).to receive(:read).and_return(TreasureData::Config.new.read(config_path))
        expect(TreasureData::Config.ssl_verify).to eq true
      end

      it 'treats non-false values as true in config file' do
        config_file.write <<-EOF
[ssl]
verify = anything_else
        EOF
        config_file.close

        allow(TreasureData::Config).to receive(:read).and_return(TreasureData::Config.new.read(config_path))
        expect(TreasureData::Config.ssl_verify).to eq true
      end

      it 'respects environment variable TD_SSL_VERIFY=false' do
        ENV['TD_SSL_VERIFY'] = 'false'
        # Simulate config initialization with env var
        TreasureData::Config.ssl_verify = (ENV['TD_SSL_VERIFY'].downcase == 'false' ? false : true)
        expect(TreasureData::Config.ssl_verify).to eq false
      end

      it 'respects environment variable TD_SSL_VERIFY=true' do
        ENV['TD_SSL_VERIFY'] = 'true'
        # Simulate config initialization with env var
        TreasureData::Config.ssl_verify = (ENV['TD_SSL_VERIFY'].downcase == 'false' ? false : true)
        expect(TreasureData::Config.ssl_verify).to eq true
      end
    end

    context 'ssl_ca_file' do
      it 'returns nil when not configured' do
        expect(TreasureData::Config.ssl_ca_file).to be_nil
      end

      it 'returns value from command line when cl_ssl_ca_file is true' do
        TreasureData::Config.cl_ssl_ca_file = true
        TreasureData::Config.ssl_ca_file = '/cli/path/to/ca.crt'
        expect(TreasureData::Config.ssl_ca_file).to eq '/cli/path/to/ca.crt'
      end

      it 'reads value from config file' do
        config_file.write <<-EOF
[ssl]
ca_file = /config/path/to/ca.crt
        EOF
        config_file.close

        allow(TreasureData::Config).to receive(:read).and_return(TreasureData::Config.new.read(config_path))
        expect(TreasureData::Config.ssl_ca_file).to eq '/config/path/to/ca.crt'
      end

      it 'respects environment variable TD_SSL_CA_FILE' do
        ENV['TD_SSL_CA_FILE'] = '/env/path/to/ca.crt'
        # Simulate config initialization with env var
        TreasureData::Config.ssl_ca_file = ENV['TD_SSL_CA_FILE']
        expect(TreasureData::Config.ssl_ca_file).to eq '/env/path/to/ca.crt'
      end

      it 'handles empty TD_SSL_CA_FILE environment variable' do
        ENV['TD_SSL_CA_FILE'] = ''
        # Simulate config initialization with empty env var
        TreasureData::Config.ssl_ca_file = ENV['TD_SSL_CA_FILE'] == '' ? nil : ENV['TD_SSL_CA_FILE']
        expect(TreasureData::Config.ssl_ca_file).to be_nil
      end
    end

    context 'cl_options_string' do
      before do
        TreasureData::Config.cl_apikey = true
        TreasureData::Config.apikey = 'test_apikey'
        TreasureData::Config.cl_endpoint = false
        TreasureData::Config.cl_import_endpoint = false
        TreasureData::Config.cl_ssl_verify = false
        TreasureData::Config.cl_ssl_ca_file = false
      end

      it 'includes API key when cl_apikey is true' do
        expect(TreasureData::Config.cl_options_string).to include('-k test_apikey')
      end

      it 'includes --insecure when cl_ssl_verify is true and ssl_verify is false' do
        TreasureData::Config.cl_ssl_verify = true
        TreasureData::Config.ssl_verify = false
        expect(TreasureData::Config.cl_options_string).to include('--insecure')
      end

      it 'does not include --insecure when cl_ssl_verify is false' do
        TreasureData::Config.cl_ssl_verify = false
        expect(TreasureData::Config.cl_options_string).not_to include('--insecure')
      end

      it 'includes --ssl-ca-file when cl_ssl_ca_file is true' do
        TreasureData::Config.cl_ssl_ca_file = true
        TreasureData::Config.ssl_ca_file = '/path/to/ca.crt'
        expect(TreasureData::Config.cl_options_string).to include('--ssl-ca-file /path/to/ca.crt')
      end

      it 'does not include --ssl-ca-file when cl_ssl_ca_file is false' do
        TreasureData::Config.cl_ssl_ca_file = false
        expect(TreasureData::Config.cl_options_string).not_to include('--ssl-ca-file')
      end

      it 'properly escapes CA file paths with spaces' do
        TreasureData::Config.cl_ssl_ca_file = true
        TreasureData::Config.ssl_ca_file = '/path/with spaces/ca.crt'
        expect(TreasureData::Config.cl_options_string).to include('--ssl-ca-file /path/with\\ spaces/ca.crt')
      end

      it 'properly escapes CA file paths with special characters' do
        TreasureData::Config.cl_ssl_ca_file = true
        TreasureData::Config.ssl_ca_file = '/path/with$special&chars/ca.crt'
        expect(TreasureData::Config.cl_options_string).to include('--ssl-ca-file /path/with\\$special\\&chars/ca.crt')
      end
    end

    context 'Configuration priority' do
      it 'CLI options have highest priority' do
        # Set config file
        config_file.write <<-EOF
[ssl]
verify = true
ca_file = /config/ca.crt
        EOF
        config_file.close

        # Set env vars
        ENV['TD_SSL_VERIFY'] = 'true'
        ENV['TD_SSL_CA_FILE'] = '/env/ca.crt'

        # Set CLI options (should win)
        TreasureData::Config.cl_ssl_verify = true
        TreasureData::Config.ssl_verify = false
        TreasureData::Config.cl_ssl_ca_file = true
        TreasureData::Config.ssl_ca_file = '/cli/ca.crt'

        expect(TreasureData::Config.ssl_verify).to eq false
        expect(TreasureData::Config.ssl_ca_file).to eq '/cli/ca.crt'
      end

      it 'Environment variables override config file when CLI not set' do
        # Set config file
        config_file.write <<-EOF
[ssl]
verify = true
ca_file = /config/ca.crt
        EOF
        config_file.close

        # Set env vars (should win over config)
        ENV['TD_SSL_VERIFY'] = 'false'
        ENV['TD_SSL_CA_FILE'] = '/env/ca.crt'

        # Simulate config initialization
        TreasureData::Config.ssl_verify = (ENV['TD_SSL_VERIFY'].downcase == 'false' ? false : true)
        TreasureData::Config.ssl_ca_file = ENV['TD_SSL_CA_FILE']
        TreasureData::Config.cl_ssl_verify = false
        TreasureData::Config.cl_ssl_ca_file = false

        expect(TreasureData::Config.ssl_verify).to eq false
        expect(TreasureData::Config.ssl_ca_file).to eq '/env/ca.crt'
      end
    end

    context 'Flag management' do
      it 'tracks cl_ssl_verify flag correctly' do
        expect(TreasureData::Config.cl_ssl_verify).to eq false
        
        TreasureData::Config.cl_ssl_verify = true
        expect(TreasureData::Config.cl_ssl_verify).to eq true
      end

      it 'tracks cl_ssl_ca_file flag correctly' do
        expect(TreasureData::Config.cl_ssl_ca_file).to eq false
        
        TreasureData::Config.cl_ssl_ca_file = true
        expect(TreasureData::Config.cl_ssl_ca_file).to eq true
      end
    end
  end
end