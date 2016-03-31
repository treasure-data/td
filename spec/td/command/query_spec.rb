# encoding: utf-8

require 'spec_helper'
require 'td/command/common'
require 'td/command/list'
require 'td/command/query'
require 'td/client'
require 'tempfile'

module TreasureData::Command
  describe 'query commands' do
    let :client do
      double('client')
    end
    let :job do
      double('job', job_id: 123)
    end
    let :command do
      Class.new { include TreasureData::Command }.new
    end
    before do
      allow(command).to receive(:get_client).and_return(client)
      expect(client).to receive(:database).with('sample_datasets')
    end

    it 'accepts --domain-key' do
      expect(client).to receive(:query).
        with("sample_datasets", "SELECT 1;", nil, nil, nil, {"domain_key"=>"hoge"}).
        and_return(job)
      op = List::CommandParser.new("query", %w[query], %w[], nil, ['--domain-key=hoge', '-dsample_datasets', 'SELECT 1;'], false)
      command.query(op)
    end

    it 'raises error if --domain-key is duplicated' do
      expect(client).to receive(:query).
        with("sample_datasets", "SELECT 1;", nil, nil, nil, {"domain_key"=>"hoge"}).
        and_raise(::TreasureData::AlreadyExistsError.new('Query failed: domain_key has already been taken'))
      op = List::CommandParser.new("query", %w[query], %w[], nil, ['--domain-key=hoge', '-dsample_datasets', 'SELECT 1;'], false)
      expect{ command.query(op) }.to raise_error(TreasureData::AlreadyExistsError)
    end
  end
end
