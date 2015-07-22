require 'spec_helper'
require 'td/command/common'
require 'td/command/list'
require 'td/command/import'

module TreasureData::Command
  describe 'import commands' do
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

    describe '#import_list' do
      let(:option) {
        List::CommandParser.new("import:list", [], [], nil, [], true)
      }
      let(:database) { 'database' }
      let(:table)    { 'table' }
      let(:response) {
        [
          double(:bulk_import,
            name: 'bulk_import',
            database: database,
            table: table,
            status: :committed,
            upload_frozen?: true,
            job_id: '123456',
            valid_parts: '',
            error_parts: '',
            valid_records: '',
            error_records: '',
          )
        ]
      }

      before do
        client = double(:client, bulk_imports: response)
        command.stub(:get_client).and_return(client)

        command.import_list(option)
      end

      it 'show import list' do
        expect(stdout_io.string).to include [database, table].join('.')
      end
    end

    describe '#import_list' do
      let(:option) {
        List::CommandParser.new("import:list", [], [], nil, [], true)
      }
      let(:database) { 'database' }
      let(:table)    { 'table' }
      let(:response) {
        [
          double(:bulk_import,
            name: 'bulk_import',
            database: database,
            table: table,
            status: :committed,
            upload_frozen?: true,
            job_id: '123456',
            valid_parts: '',
            error_parts: '',
            valid_records: '',
            error_records: '',
          )
        ]
      }

      before do
        client = double(:client, bulk_imports: response)
        command.stub(:get_client).and_return(client)

        command.import_list(option)
      end

      it 'show import list' do
        expect(stdout_io.string).to include [database, table].join('.')
      end
    end

    describe '#import_show' do
      let(:import_name) { 'bulk_import' }
      let(:option) {
        List::CommandParser.new("import:show", ['name'], [], nil, [import_name], true)
      }
      let(:client) { double(:client) }

      before do
        command.stub(:get_client).and_return(client)
      end

      context 'not exists import' do
        it 'should be error' do
          client.should_receive(:bulk_import).with(import_name).and_return(nil)
          command.should_receive(:exit).with(1).and_return { raise CallSystemExitError }

          expect {
            command.import_show(option)
          }.to raise_error CallSystemExitError
        end
      end

      context 'exist import' do
        let(:import_name) { 'bulk_import' }
        let(:bulk_import) {
          double(:bulk_import,
            name: import_name,
            database: 'database',
            table: 'table',
            status: :committed,
            upload_frozen?: true,
            job_id: '123456',
            valid_parts: '',
            error_parts: '',
            valid_records: '',
            error_records: '',
          )
        }
        let(:bulk_import_parts) {
          %w(part1 part2 part3)
        }

        before do
          client.should_receive(:bulk_import).with(import_name).and_return(bulk_import)
          client.should_receive(:list_bulk_import_parts).with(import_name).and_return(bulk_import_parts)

          command.import_show(option)
        end

        it 'stderr should be include import name' do
          expect(stderr_io.string).to include import_name
        end

        it 'stdout should be include import parts' do
          bulk_import_parts.each do |part|
            expect(stdout_io.string).to include part
          end
        end
      end
    end
  end
end
