require 'spec_helper'
require 'tempfile'
require 'td/config'

describe TreasureData::Config do
  describe 'SSL options' do
    context 'ssl_verify' do
      before do
        TreasureData::Config.cl_ssl_verify = false
        TreasureData::Config.ssl_verify = true
        ENV.delete('TD_SSL_VERIFY')
      end

      it 'returns default value when not set' do
        expect(TreasureData::Config.ssl_verify).to eq true
      end

      it 'returns value from command line when set' do
        TreasureData::Config.cl_ssl_verify = true
        TreasureData::Config.ssl_verify = false
        expect(TreasureData::Config.ssl_verify).to eq false
      end

      it 'reads value from config file' do
        Tempfile.create('td.conf') do |f|
          f << <<-EOF
[ssl]
verify=false
          EOF
          f.close

          allow(TreasureData::Config).to receive(:read).and_return(TreasureData::Config.new.read(f.path))
          expect(TreasureData::Config.ssl_verify).to eq false
        end
      end
    end

    context 'ssl_ca_file' do
      before do
        TreasureData::Config.cl_ssl_ca_file = false
        TreasureData::Config.ssl_ca_file = nil
        ENV.delete('TD_SSL_CA_FILE')
      end

      it 'returns nil when not set' do
        expect(TreasureData::Config.ssl_ca_file).to be_nil
      end

      it 'returns value from command line when set' do
        TreasureData::Config.cl_ssl_ca_file = true
        TreasureData::Config.ssl_ca_file = '/path/to/ca.crt'
        expect(TreasureData::Config.ssl_ca_file).to eq '/path/to/ca.crt'
      end

      it 'reads value from config file' do
        Tempfile.create('td.conf') do |f|
          f << <<-EOF
[ssl]
ca_file=/path/to/ca.crt
          EOF
          f.close

          allow(TreasureData::Config).to receive(:read).and_return(TreasureData::Config.new.read(f.path))
          expect(TreasureData::Config.ssl_ca_file).to eq '/path/to/ca.crt'
        end
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

      it 'includes insecure option when ssl_verify is false' do
        TreasureData::Config.cl_ssl_verify = true
        TreasureData::Config.ssl_verify = false
        expect(TreasureData::Config.cl_options_string).to include('--insecure')
      end

      it 'includes ssl_ca_file option when set' do
        TreasureData::Config.cl_ssl_ca_file = true
        TreasureData::Config.ssl_ca_file = '/path/to/ca.crt'
        expect(TreasureData::Config.cl_options_string).to include('--ssl-ca-file /path/to/ca.crt')
      end

      it 'escapes ssl_ca_file path with spaces' do
        TreasureData::Config.cl_ssl_ca_file = true
        TreasureData::Config.ssl_ca_file = '/path/to/my ca.crt'
        expect(TreasureData::Config.cl_options_string).to include('--ssl-ca-file /path/to/my\\ ca.crt')
      end
    end
  end
end
