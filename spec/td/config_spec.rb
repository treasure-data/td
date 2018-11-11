require 'spec_helper'
require 'tempfile'
require 'td/config'

describe TreasureData::Config do
  context 'workflow_endpoint' do
    before { TreasureData::Config.endpoint = api_endpoint }
    subject { TreasureData::Config.workflow_endpoint }
    context 'api.treasuredata.com' do
      context 'works without http schema' do
        let(:api_endpoint){ 'api.treasuredata.com' }
        it { is_expected.to eq 'https://api-workflow.treasuredata.com' }
      end
      context 'works with http schema' do
        let(:api_endpoint){ 'http://api.treasuredata.com' }
        it { is_expected.to eq 'https://api-workflow.treasuredata.com' }
      end
      context 'works with https schema' do
        let(:api_endpoint){ 'https://api.treasuredata.com' }
        it { is_expected.to eq 'https://api-workflow.treasuredata.com' }
      end
    end
    context 'api.treasuredata.co.jp' do
      let(:api_endpoint){ 'api.treasuredata.co.jp' }
      it { is_expected.to eq 'https://api-workflow.treasuredata.co.jp' }
    end
    context 'api.eu01.treasuredata.com' do
      let(:api_endpoint){ 'api.eu01.treasuredata.com' }
      it { is_expected.to eq 'https://api-workflow.eu01.treasuredata.com' }
    end

    context 'api-hoge.connect.treasuredata.com' do
      let(:api_endpoint){ 'api-hoge.connect.treasuredata.com' }
      it { is_expected.to eq 'https://api-workflow-hoge.connect.treasuredata.com' }
    end
    context 'api-hoge.connect.treasuredata.co.jp' do
      let(:api_endpoint){ 'api-hoge.connect.treasuredata.co.jp' }
      it { is_expected.to eq 'https://api-workflow-hoge.connect.treasuredata.co.jp' }
    end

    context 'api-staging.treasuredata.com' do
      let(:api_endpoint){ 'api-staging.treasuredata.com' }
      it { is_expected.to eq 'https://api-staging-workflow.treasuredata.com' }
    end
    context 'api-staging.treasuredata.co.jp' do
      let(:api_endpoint){ 'api-staging.treasuredata.co.jp' }
      it { is_expected.to eq 'https://api-staging-workflow.treasuredata.co.jp' }
    end
    context 'api-staging.eu01.treasuredata.com' do
      let(:api_endpoint){ 'api-staging.eu01.treasuredata.com' }
      it { is_expected.to eq 'https://api-staging-workflow.eu01.treasuredata.com' }
    end

    context 'api-development.treasuredata.com' do
      let(:api_endpoint){ 'api-development.treasuredata.com' }
      it { is_expected.to eq 'https://api-development-workflow.treasuredata.com' }
    end
    context 'api-development.treasuredata.co.jp' do
      let(:api_endpoint){ 'api-development.treasuredata.co.jp' }
      it { is_expected.to eq 'https://api-development-workflow.treasuredata.co.jp' }
    end
    context 'api-development.eu01.treasuredata.com' do
      let(:api_endpoint){ 'api-development.eu01.treasuredata.com' }
      it { is_expected.to eq 'https://api-development-workflow.eu01.treasuredata.com' }
    end

    context 'ybi.jp-east.idcfcloud.com' do
      let(:api_endpoint){ 'ybi.jp-east.idcfcloud.com' }
      it 'raise error' do
        expect { subject }.to raise_error(TreasureData::ConfigError)
      end
    end
  end

  describe '#read' do
    it 'sets @conf' do
      Tempfile.create('td.conf') do |f|
        f << <<-EOF
# This is comment
[section1]
# This is comment
key=val
foo=bar
        EOF

        f.close

        config = TreasureData::Config.new
        config.read(f.path)

        expect(config["section1.key"]).to eq "val"
        expect(config["section1.foo"]).to eq "bar"
      end
    end
  end
end
