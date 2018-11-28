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

    describe 'domain key' do
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

    describe 'wait' do
      let (:job_id){ 123 }
      let :job do
        obj = TreasureData::Job.new(client, job_id, 'presto', 'SELECT 1;')
        allow(obj).to receive(:debug).and_return(double.as_null_object)
        allow(obj).to receive(:sleep){|arg|@now += arg}
        obj
      end
      before do
        expect(client).to receive(:query).
          with("sample_datasets", "SELECT 1;", nil, nil, nil, {}).
          and_return(job)
        @now = 1_400_000_000
        allow(Time).to receive(:now){ @now }
        allow(command).to receive(:show_result_with_retry)
      end
      context 'success' do
        before do
          status = nil
          allow(client).to receive_message_chain(:api, :show_job).with(job_id) do
            if @now < 1_400_000_010
              status = TreasureData::Job::STATUS_RUNNING
            else
              status = TreasureData::Job::STATUS_SUCCESS
            end
            nil
          end
          allow(client).to receive(:job_status).with(job_id){ status }
        end
        it 'works with --wait' do
          op = List::CommandParser.new("query", %w[query], %w[], nil, ['--wait', '-dsample_datasets', 'SELECT 1;'], false)
          expect(command.query(op)).to be_nil
        end
        it 'works with --wait=360' do
          op = List::CommandParser.new("query", %w[query], %w[], nil, ['--wait=360', '-dsample_datasets', 'SELECT 1;'], false)
          expect(command.query(op)).to be_nil
        end
      end
      context 'temporary failure' do
        before do
          status = nil
          allow(client).to receive_message_chain(:api, :show_job).with(job_id) do
            if @now < 1_400_000_010
              status = TreasureData::Job::STATUS_RUNNING
            else
              status = TreasureData::Job::STATUS_SUCCESS
            end
            nil
          end
          allow(client).to receive(:job_status).with(job_id){ status }
        end
        it 'works with --wait' do
          op = List::CommandParser.new("query", %w[query], %w[], nil, ['--wait', '-dsample_datasets', 'SELECT 1;'], false)
          expect(command.query(op)).to be_nil
        end
        it 'works with --wait=360' do
          op = List::CommandParser.new("query", %w[query], %w[], nil, ['--wait=360', '-dsample_datasets', 'SELECT 1;'], false)
          expect(command.query(op)).to be_nil
        end
      end
    end

    describe 'engine version' do
      it 'accepts --engine-version' do
        expect(client).to receive(:query).
          with("sample_datasets", "SELECT 1;", nil, nil, nil, {"engine_version"=>"stable"}).
          and_return(job)
        op = List::CommandParser.new("query", %w[query], %w[], nil, ['--engine-version=stable', '-dsample_datasets', 'SELECT 1;'], false)
        command.query(op)
      end
    end
  end
end
