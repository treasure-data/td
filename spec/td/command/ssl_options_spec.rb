require 'spec_helper'
require 'td/config'
require 'td/command/common'
require 'td/command/runner'
require 'tempfile'

module TreasureData
  module Command
    describe 'SSL Options' do

      let(:command) { Class.new { include TreasureData::Command }.new }
      let(:config_path) { Tempfile.new('td-config').path }
      let(:runner) { TreasureData::Command::Runner.new }

      before do
        Config.path = config_path
        Config.apikey = "dummy"
        Config.cl_apikey = true
        Config.ssl_verify = true
        Config.ssl_ca_file = nil
        Config.cl_ssl_verify = false
        Config.cl_ssl_ca_file = false
      end

      after do
        File.unlink(config_path) if File.exist?(config_path)
      end

      describe 'SSL verification options' do
        it 'disables verification with --insecure' do
          runner.run(['--insecure', 'status'])
          expect(Config.ssl_verify).to be false
        end

        it 'sets custom CA file with --ssl-ca-file' do
          ca_file = Tempfile.new('ca-cert').path
          File.write(ca_file, "dummy cert")
          
          runner.run(['--ssl-ca-file', ca_file, 'status'])
          expect(Config.ssl_ca_file).to eq ca_file
          
          File.unlink(ca_file)
        end

        it 'reads SSL options from config file' do
          File.open(config_path, 'w') do |f|
            f.puts "[ssl]"
            f.puts "verify = false"
            f.puts "ca_file = /path/to/ca.crt"
          end

          # Reset flags to ensure we read from config
          Config.cl_ssl_verify = false
          Config.cl_ssl_ca_file = false
          
          expect(Config.ssl_verify).to be false
          expect(Config.ssl_ca_file).to eq '/path/to/ca.crt'
        end

        
        it 'respects priority order: CLI > env > config' do
          Config.cl_ssl_verify = false
          Config.cl_ssl_ca_file = false
        
          File.open(config_path, 'w') do |f|
            f.puts "[ssl]"
            f.puts "verify = false"
            f.puts "ca_file = /path/from/config.crt"
          end

          expect(Config.ssl_verify).to be false
          expect(Config.ssl_ca_file).to eq '/path/from/config.crt'

          ENV['TD_SSL_VERIFY'] = 'true'
          ENV['TD_SSL_CA_FILE'] = '/path/from/env.crt'
        
        
          ca_file = Tempfile.new('ca-cert').path
          File.write(ca_file, "dummy cert")
        
          runner.run(['--ssl-ca-file', ca_file, '--insecure', 'status'])
          expect(Config.ssl_verify).to be false
          expect(Config.ssl_ca_file).to eq ca_file
        
          File.unlink(ca_file)
        
          ENV.delete('TD_SSL_VERIFY')
          ENV.delete('TD_SSL_CA_FILE')
        end

      describe 'Client creation with SSL options' do
        it 'passes verify=false to client when --insecure is used' do
          runner.run(['--insecure', 'status'])
          expect(TreasureData::Client).to receive(:new).with("dummy", hash_including(verify: false)).and_return(double('client').as_null_object)
          
          command.send(:get_client)
        end

          it 'passes verify=ca_file to client when --ssl-ca-file is used' do
            ca_file = Tempfile.new('ca-cert').path
            File.write(ca_file, "dummy cert")
          
            runner.run(['--ssl-ca-file', ca_file, 'status'])
          
            expect(TreasureData::Client).to receive(:new).with("dummy", hash_including(verify: ca_file)).and_return(double('client').as_null_object)
          
            command.send(:get_client)
          
            File.unlink(ca_file)
          end
        end
      end
    end
  end
end
