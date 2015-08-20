require 'spec_helper'
require 'td/command/common'
require 'td/command/list'
require 'td/command/connector'

module TreasureData::Command
  describe 'connector commands' do
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

      describe 'database and table arguments' do
        let(:database) { 'database' }
        let(:table)    { 'table' }

        before do
          client = double(:client)
          command.stub(:get_client).and_return(client)
          command.stub(:create_database_and_table_if_not_exist)
          command.stub(:prepare_bulkload_job_config).and_return(config)
          client.should_receive(:bulk_load_issue).with(database, table, {config: expect_config}).and_return(1234)
        end

        context 'set from config file' do
          let(:expect_config) {
            {'out' => {'database' => database, 'table' => table}}
          }

          context 'without arguments' do
            let(:option) {
              List::CommandParser.new("connector:issue", ["config"], [], nil, [File.join("spec", "td", "fixture", "bulk_load.yml")], true)
            }
            let(:config) {
              {'out' => {'database' => database, 'table' => table}}
            }

            it { expect { subject }.not_to raise_error }
          end

          context 'with arguments' do
            let(:option) {
              List::CommandParser.new("connector:issue", ["config"], ['database', 'table'], nil, [File.join("spec", "td", "fixture", "bulk_load.yml"), '--database', database, '--table', table], true)
            }
            let(:config) {
              {'out' => {'database' => 'config_database', 'table' => 'config_table'}}
            }

            it { expect { subject }.not_to raise_error }
          end
        end

        context 'set --database and --table' do
          let(:option) {
            List::CommandParser.new("connector:issue", ["config"], ['database', 'table'], nil, [File.join("spec", "td", "fixture", "bulk_load.yml"), '--database', database, '--table', table], true)
          }
          let(:config)   { {} }
          let(:expect_config) {
            {'out' => {'database' => database, 'table' => table}}
          }

          it 'show warning' do
            subject

            expect(stderr_io.string).to include '--database is obsolete option'
            expect(stderr_io.string).to include '--table is obsolete option'
          end
        end

        context 'set arguments' do
          let(:option) {
            List::CommandParser.new("connector:issue", ["config", 'database', 'table'], [], nil, [database, table, File.join("spec", "td", "fixture", "bulk_load.yml")], true)
          }
          let(:config)   { {} }
          let(:expect_config) {
            {'out' => {'database' => database, 'table' => table}}
          }

          it 'no warning' do
            subject

            expect(stderr_io.string).not_to include '--database is obsolete option'
            expect(stderr_io.string).not_to include '--table is obsolete option'
          end
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
