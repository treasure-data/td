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
  end
end
