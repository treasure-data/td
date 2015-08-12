require 'spec_helper'
require 'td/command/common'
require 'td/command/list'
require 'td/command/connector'

module TreasureData::Command
  describe 'connector commands' do
    describe '#connector_init' do
      include_context 'quiet_out'

      let :command do
        Class.new { include TreasureData::Command }.new
      end
      let :option do
        List::CommandParser.new("connector:init", ["url"], [], nil, arguments, true)
      end
      let :out_file do
        Tempfile.new("seed.yml").tap {|s| s.close }
      end

      before do
        command.connector_init(option)
      end

      subject(:generated_config) { YAML.load_file(out_file.path) }

      %w(csv tsv).each do |format|
        context "--format #{format}" do
          context 'use path' do
            let(:path_prefix)   { 'path/to/prefix_' }
            let(:arguments)     { ["#{path_prefix}*.#{format}", '--format', format, '-o', out_file.path] }

            it 'generate configuration file' do
              expect(generated_config).to eq({
                'in' => {
                  'type'              => 's3',
                  'access_key_id'     => '',
                  'secret_access_key' => '',
                  'bucket'            => '',
                  'path_prefix'       => path_prefix,
                },
                'out' => {
                  'mode' => 'append'
                }
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
    end

    describe '#connector_guess' do
      let :command do
        Class.new { include TreasureData::Command }.new
      end

      describe 'guess plugins' do
        let(:guess_plugins) { %w(json query_string) }
        let(:in_file)  { Tempfile.new('in.yml').tap{|f| f.close } }
        let(:out_file) { Tempfile.new('out.yml').tap{|f| f.close } }
        let(:option) {
          List::CommandParser.new("connector:guess", ["config"], [], nil, [in_file.path, '-o', out_file.path, '--guess', guess_plugins.join(',')], true)
        }
        let(:client) { double(:client) }

        before do
          command.stub(:get_client).and_return(client)
        end

        let(:config) {
          {
            'in' => {'type' => 's3'}
          }
        }
        let(:expect_config) {
          config.merge('out' => {}, 'exec' => {'guess_plugins' => guess_plugins})
        }

        include_context 'quiet_out'

        before do
          command.stub(:prepare_bulkload_job_config).and_return(config)
        end

        it 'guess_plugins passed td-client' do
          client.should_receive(:bulk_load_guess).with({config: expect_config}).and_return({})

          command.connector_guess(option)
        end
      end
    end

    describe '#connector_preview' do
      let :command do
        Class.new { include TreasureData::Command }.new
      end

      subject do
        backup = $stdout.dup
        buf = StringIO.new

        begin
          $stdout = buf

          op = List::CommandParser.new("connector:preview", ["config"], [], nil, [File.join("spec", "td", "fixture", "bulk_load.yml")], true)
          command.connector_preview(op)

          buf.string
        ensure
          $stdout = backup
        end
      end

      let(:preview_result) do
        {
          "schema" => [
            {"index" => 0, "name" => "c0_too_l#{'o' * 60}ng_column_name", "type" => "string"},
            {"index" => 1, "name" => "c1", "type" => "long"},
            {"index" => 2, "name" => "c2", "type" => "string"},
            {"index" => 3, "name" => "c3", "type" => "string"}
          ],
          "records" => [
            ["19920116", 32864, "06612", "00195"],
            ["19910729", 14824, "07706", "00058"],
            ["19881022", 26114, "06960", "00175"]
          ]
        }
      end

      before do
        client = double(:client, bulk_load_preview: preview_result)
        command.stub(:get_client).and_return(client)
      end

      it 'should include too_long_column_name without truncated' do
        too_long_column_name = preview_result["schema"][0]["name"]
        expect(subject).to include "#{too_long_column_name}:string"
      end
    end

    describe '#connector_issue' do
      let :command do
        Class.new { include TreasureData::Command }.new
      end

      let(:stderr_io) do
        StringIO.new
      end

      subject do
        backup = $stdout.dup
        stderr_backup = $stderr.dup
        buf = StringIO.new

        begin
          $stdout = buf
          $stderr = stderr_io

          command.connector_issue(option)

          buf.string
        ensure
          $stdout = backup
          $stderr = stderr_backup
        end
      end

      describe 'queueing job' do
        let(:option) {
          List::CommandParser.new("connector:issue", ["config"], ['database', 'table'], nil, [File.join("spec", "td", "fixture", "bulk_load.yml"), '--database', 'database', '--table', 'table'], true)
        }

        before do
          client = double(:client, bulk_load_issue: 1234)
          command.stub(:get_client).and_return(client)
          command.stub(:create_database_and_table_if_not_exist)
        end

        it 'should include too_long_column_name without truncated' do
          expect(subject).to include "Job 1234 is queued."
        end
      end

      describe 'distination table' do
        let(:client) { double(:client, bulk_load_issue: 1234) }

        before do
          command.stub(:get_client).and_return(client)
          client.stub(:database)
        end

        context 'set auto crate table option' do
          let(:option) {
            List::CommandParser.new("connector:issue", ["config"], ['database', 'table'], nil, [File.join("spec", "td", "fixture", "bulk_load.yml"), '--database', 'database', '--table', 'table', '--auto-create-table'], true)
          }

          it 'call create_database_and_table_if_not_exist' do
            command.should_receive(:create_database_and_table_if_not_exist)

            subject
          end
        end

        context 'not set auto crate table option' do
          let(:option) {
            List::CommandParser.new("connector:issue", ["config"], ['database', 'table'], nil, [File.join("spec", "td", "fixture", "bulk_load.yml"), '--database', 'database', '--table', 'table'], true)
          }

          it 'call create_database_and_table_if_not_exist' do
            command.should_not_receive(:create_database_and_table_if_not_exist)

            subject
          end
        end
      end
    end
  end
end
