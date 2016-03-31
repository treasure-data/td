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
        allow(command).to receive(:get_client).and_return(client)

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
        allow(command).to receive(:get_client).and_return(client)

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
        allow(command).to receive(:get_client).and_return(client)
      end

      context 'not exists import' do
        it 'should be error' do
          expect(client).to receive(:bulk_import).with(import_name).and_return(nil)
          expect(command).to receive(:exit).with(1) { raise CallSystemExitError }

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
          expect(client).to receive(:bulk_import).with(import_name).and_return(bulk_import)
          expect(client).to receive(:list_bulk_import_parts).with(import_name).and_return(bulk_import_parts)

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

    describe '#import_create' do
      let(:import_name) { 'bulk_import' }
      let(:database) { 'database' }
      let(:table) { 'table' }
      let(:option) {
        List::CommandParser.new("import:create", %w(name, db_name, table_name), [], nil, [import_name, database, table], true)
      }
      let(:client) { double(:client) }

      before do
        allow(command).to receive(:get_client).and_return(client)
      end

      it 'create bulk import' do
        expect(client).to receive(:create_bulk_import).with(import_name, database, table, {})

        command.import_create(option)

        expect(stderr_io.string).to include import_name
      end
    end

    describe '#import_config' do
      include_context 'quiet_out'

      let :command do
        Class.new { include TreasureData::Command }.new
      end
      let :option do
        List::CommandParser.new("import:config", args, [], nil, arguments, true)
      end
      let :out_file do
        Tempfile.new("seed.yml").tap {|s| s.close }
      end
      let(:endpoint) { 'http://example.com' }
      let(:apikey) { '1234ABCDEFGHIJKLMN' }

      before do
        allow(TreasureData::Config).to receive(:endpoint).and_return(endpoint)
        allow(TreasureData::Config).to receive(:apikey).and_return(apikey)
      end

      context 'unknown format' do
        let(:args) { ['url'] }
        let(:arguments) { ['localhost', '--format', 'msgpack', '-o', out_file.path] }

        it 'exit command' do
          expect {
            command.import_config(option)
          }.to raise_error TreasureData::Command::ParameterConfigurationError
        end
      end

      context 'support format' do
        let(:td_output_config) {
          {
            'type' => 'td',
            'endpoint' => TreasureData::Config.endpoint,
            'apikey' =>  TreasureData::Config.apikey,
            'database' => '',
            'table' => '',
          }
        }

        before do
          command.import_config(option)
        end

        subject(:generated_config) { YAML.load_file(out_file.path) }

        %w(csv tsv).each do |format|
          context "--format #{format}" do
            let(:args) { ['url'] }
            context 'use path' do
              let(:path_prefix)   { 'path/to/prefix_' }
              let(:arguments)     { ["#{path_prefix}*.#{format}", '--format', format, '-o', out_file.path] }

              it 'generate configuration file' do
                expect(generated_config).to eq({
                  'in' => {
                    'type'        => 'file',
                    'decorders'   => [{'type' => 'gzip'}],
                    'path_prefix' => path_prefix,
                  },
                  'out' => td_output_config
                })
              end
            end

            context 'use s3 scheme' do
              let(:s3_access_key) { 'ABCDEFGHIJKLMN' }
              let(:s3_secret_key) { '1234ABCDEFGHIJKLMN' }
              let(:buckt_name)    { 'my_bucket' }
              let(:path_prefix)   { 'path/to/prefix_' }
              let(:s3_url)        { "s3://#{s3_access_key}:#{s3_secret_key}@/#{buckt_name}/#{path_prefix}*.#{format}" }
              let(:arguments)     { [s3_url, '--format', format, '-o', out_file.path] }

              it 'generate configuration file' do
                expect(generated_config).to eq({
                  'in' => {
                    'type'              => 's3',
                    'access_key_id'     => s3_access_key,
                    'secret_access_key' => s3_secret_key,
                    'bucket'            => buckt_name,
                    'path_prefix'       => path_prefix,
                  },
                  'out' => {
                    'mode' => 'append'
                  }
                })
              end
            end
          end
        end

        context 'format is mysql' do
          let(:format)   { 'mysql' }
          let(:host)     { 'localhost' }
          let(:database) { 'database' }
          let(:user)     { 'my_user' }
          let(:password) { 'my_password' }
          let(:table)    { 'my_table' }

          let(:expected_config) {
            {
              'in' => {
                'type'     => 'mysql',
                'host'     => host,
                'port'     => port,
                'database' => database,
                'user'     => user,
                'password' => password,
                'table'    => table,
                'select'   => "*",
              },
              'out' => td_output_config
            }
          }

          context 'like import:prepare arguments' do
            let(:args) { ['url'] }
            let(:arguments) { [table, '--db-url', mysql_url, '--db-user', user, '--db-password', password, '--format', 'mysql', '-o', out_file.path] }

            context 'scheme is jdbc' do
              let(:port)      { 3333 }
              let(:mysql_url) { "jdbc:mysql://#{host}:#{port}/#{database}" }

              it 'generate configuration file' do
                expect(generated_config).to eq expected_config
              end
            end

            context 'scheme is mysql' do
              context 'with port' do
                let(:port)      { 3333 }
                let(:mysql_url) { "mysql://#{host}:#{port}/#{database}" }

                it 'generate configuration file' do
                  expect(generated_config).to eq expected_config
                end
              end

              context 'without port' do
                let(:mysql_url) { "mysql://#{host}/#{database}" }
                let(:port) { 3306 }

                it 'generate configuration file' do
                  expect(generated_config).to eq expected_config
                end
              end
            end
          end

          context 'like import:upload arguments' do
            let(:args)      { ['session', 'url'] }
            let(:arguments) { ['session', table, '--db-url', mysql_url, '--db-user', user, '--db-password', password, '--format', 'mysql', '-o', out_file.path] }
            let(:mysql_url) { "jdbc:mysql://#{host}/#{database}" }
            let(:port)      { 3306 }

            it 'generate configuration file' do
              expect(generated_config).to eq expected_config
            end
          end
        end
      end

      context 'not migrate options' do
        %w(--columns --column-header).each do |opt|
          context "with #{opt}" do
            let(:args) { ['url'] }
            let(:arguments)     { ["path/to/prefix_*.csv", '--format', 'csv', opt, 'col1,col2', '-o', out_file.path] }

            it "#{opt} is not migrate" do
              expect { command.import_config(option) }.not_to raise_error
              expect(stderr_io.string).to include 'not migrate. Please, edit config file after execute guess commands.'
            end
          end
        end
      end
    end
  end
end
