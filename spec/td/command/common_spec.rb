require 'spec_helper'
require 'td/command/common'
require 'td/config'

module TreasureData::Command
  describe 'get_client' do
    let(:command) { Object.new.extend(TreasureData::Command) }
    
    before do
      allow(TreasureData::Config).to receive(:apikey).and_return('test_apikey')
      allow(TreasureData::Config).to receive(:secure).and_return(true)
      allow(TreasureData::Config).to receive(:retry_post_requests).and_return(false)
      allow(TreasureData::Config).to receive(:endpoint).and_return(nil)
      allow(TreasureData::Config).to receive(:ssl_verify).and_return(true)
      allow(TreasureData::Config).to receive(:ssl_ca_file).and_return(nil)
    end
    
    it 'passes ssl_verify=false to client when ssl_verify is false' do
      allow(TreasureData::Config).to receive(:ssl_verify).and_return(false)
      expect(TreasureData::Client).to receive(:new).with('test_apikey', hash_including(verify: false))
      command.get_client
    end
    
    it 'passes ssl_ca_file to client when ssl_ca_file is set' do
      allow(TreasureData::Config).to receive(:ssl_ca_file).and_return('/path/to/ca.crt')
      expect(TreasureData::Client).to receive(:new).with('test_apikey', hash_including(verify: '/path/to/ca.crt'))
      command.get_client
    end
    
    it 'does not set verify option when ssl_verify is true and ssl_ca_file is not set' do
      allow(TreasureData::Config).to receive(:ssl_verify).and_return(true)
      allow(TreasureData::Config).to receive(:ssl_ca_file).and_return(nil)
      expect(TreasureData::Client).to receive(:new).with('test_apikey', hash_not_including(:verify))
      command.get_client
    end
  end
end