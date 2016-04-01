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
      let(:option_list) { [database, table, "-b", bucket, "-p", path, "-k", key, "-s", pass, "-F", format] }
      let(:database) { 'database' }
      let(:table)    { 'table' }
      let(:bucket)    { 'bucket' }
      let(:path)    { 'path' }
      let(:key)    { 'key' }
      let(:pass)    { 'pass' }
      let(:format)    { 'tsv.gz' }
      let(:encryption)    { 's3' }
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
    end
  end
end
