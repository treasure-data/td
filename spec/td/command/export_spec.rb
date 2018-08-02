require 'spec_helper'
require 'td/command/common'
require 'td/command/list'
require 'td/command/export'

module TreasureData::Command
  describe 'export commands' do
    let(:command) {
      Class.new { include TreasureData::Command }.new
    }
    let(:stdout_io) { StringIO.new }
    let(:stderr_io) { StringIO.new }

    around do |example|
      stdout = $stdout.dup
      stderr = $stderr.dup

      begin
        $stdout = stdout_io
        $stderr = stderr_io

        example.run
      ensure
        $stdout = stdout
        $stderr = stderr
      end
    end

    describe '#table_export' do
      let(:option) {
        List::CommandParser.new("table:export", ["db_name", "table_name"], [], nil, option_list, true)
      }
      let(:option_with_encryption) {
        ops = option_list
        ops.push "-e"
        ops.push encryption
        List::CommandParser.new("table:export", ["db_name", "table_name"], [], nil, ops, true)
      }
      let(:option_with_wrong_encryption) {
        ops = option_list
        ops.push "-e"
        ops.push wrong_encryption
        List::CommandParser.new("table:export", ["db_name", "table_name"], [], nil, ops, true)
      }
      let(:option_with_assume_role) {
        ops = option_list
        ops.push "-a"
        ops.push assume_role
        List::CommandParser.new("table:export", ["db_name", "table_name"], [], nil, ops, true)
      }
      let(:option_list) { [database, table, "-b", bucket, "-p", path, "-k", key, "-s", pass, "-F", format] }
      let(:database) { 'database' }
      let(:table)    { 'table' }
      let(:bucket)    { 'bucket' }
      let(:path)    { 'path' }
      let(:key)    { 'key' }
      let(:pass)    { 'pass' }
      let(:format)    { 'tsv.gz' }
      let(:encryption)    { 's3' }
      let(:assume_role)    { 'arn:aws:iam::000:role/assume' }
      let(:wrong_encryption)    { 's3s3' }
      let(:job_id)    { 111 }

      before do
        client = double(:client)
        job = double(:job, job_id: job_id)
        allow(client).to receive(:export).and_return(job)
        table = double(:table)

        allow(command).to receive(:get_client).and_return(client)
        allow(command).to receive(:get_table).and_return(table)
      end

      it 'export table without encryption' do
        expect {
          command.table_export(option)
        }.to_not raise_exception
      end

      it 'export table with encryption' do
        expect {
          command.table_export(option_with_encryption)
        }.to_not raise_exception
      end

      it 'fail to export table with wrong encryption' do
        expect {
          command.table_export(option_with_wrong_encryption)
        }.to raise_exception
      end

      it 'export table without assume role' do
        expect {
          command.table_export(option_with_assume_role)
        }.to_not raise_exception
      end

      it 'export table with assume role' do
        expect {
          command.table_export(option_with_assume_role)
        }.to_not raise_exception
      end
    end

    describe '#export_table' do
      let(:option) {
        List::CommandParser.new("export:table", ["db_name", "table_name"], [], nil, option_list, true)
      }
      let(:option_list) { [database, table, "-b", bucket, "-p", path, "-k", key, "-s", pass, "-F", format] }
      let(:database) { 'database' }
      let(:table)    { 'table' }
      let(:bucket)    { 'bucket' }
      let(:path)    { 'path' }
      let(:key)    { 'key' }
      let(:pass)    { 'pass' }
      let(:format)    { 'tsv.gz' }
      let(:job_id)    { 111 }

      before do
        client = double(:client)
        job = double(:job, job_id: job_id)
        allow(client).to receive(:export).and_return(job)
        table = double(:table)

        allow(command).to receive(:get_client).and_return(client)
        allow(command).to receive(:get_table).and_return(table)
      end

      it 'export table successfully like #table_export' do
        expect {
          command.table_export(option)
        }.to_not raise_exception
      end
    end

    describe '#export_result' do
      let(:option) {
        List::CommandParser.new("export:result", ["target_job_id", "result_url"], [], nil, option_list, true)
      }
      let(:option_with_retry) {
        List::CommandParser.new("export:result", ["target_job_id", "result_url"], [], nil, ['-R', '3'] + option_list, true)
      }
      let(:option_with_priority) {
        List::CommandParser.new("export:result", ["target_job_id", "result_url"], [], nil, ['-P', '-2'] + option_list, true)
      }
      let(:option_with_wrong_priority) {
        List::CommandParser.new("export:result", ["target_job_id", "result_url"], [], nil, ['-P', '3'] + option_list, true)
      }
      let(:option_with_wait) {
        List::CommandParser.new("export:result", ["target_job_id", "result_url"], [], nil, ['-w'] + option_list, true)
      }
      let(:option_list) { [110, 'mysql://user:pass@host.com/database/table'] }
      let(:job_id)    { 111 }
      let(:client)    { double(:client) }
      let(:job)       { double(:job, job_id: job_id) }

      before do
        allow(client).to receive(:result_export).and_return(job)

        allow(command).to receive(:get_client).and_return(client)
      end

      it 'export result successfully' do
        expect {
          command.export_result(option)
        }.not_to raise_exception
      end

      it 'works with retry option' do
        expect {
          command.export_result(option_with_retry)
        }.not_to raise_exception
      end

      it 'works with priority option' do
        expect {
          command.export_result(option_with_priority)
        }.not_to raise_exception
      end

      it 'detects wrong priority option' do
        expect {
          command.export_result(option_with_wrong_priority)
        }.to raise_exception
      end

      it 'detects wait option' do
        target_job = double('target_job')
        expect(client).to receive(:job).and_return(target_job)
        count_target_job_finished_p = 0
        expect(target_job).to receive(:finished?).and_return(false)
        allow(target_job).to receive(:wait)
        allow(target_job).to receive(:status)
        allow(job).to receive(:wait)
        allow(job).to receive(:status)
        command.export_result(option_with_wait)
      end
    end
  end
end
