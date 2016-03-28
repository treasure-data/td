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
        List::CommandParser.new("table:export", ["db_name", "table_name"], [], nil, [database, table, "-b", bucket, "-p", path, "-k", key, "-s", pass, "-F", format], true)
      }
      let(:option_with_encryption) {
        List::CommandParser.new("table:export", ["db_name", "table_name"], [], nil, [database, table, "-b", bucket, "-p", path, "-k", key, "-s", pass, "-F", format, "--encryption", encryption], true)
      }
      let(:database) { 'database' }
      let(:table)    { 'table' }
      let(:bucket)    { 'bucket' }
      let(:path)    { 'path' }
      let(:key)    { 'key' }
      let(:pass)    { 'pass' }
      let(:format)    { 'tsv.gz' }
      let(:encryption)    { 's3' }
      let(:job_id)    { 111 }

      before do
        client = double(:client)
        job = double(:job, job_id: job_id)
        client.stub(:export).and_return(job)
        table = double(:table)

        command.stub(:get_client).and_return(client)
        command.stub(:get_table).and_return(table)
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
    end
  end
end
