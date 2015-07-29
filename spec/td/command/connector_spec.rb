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

          op = List::CommandParser.new("connector:issue", ["config"], ['database', 'table'], nil, [File.join("spec", "td", "fixture", "bulk_load.yml"), '--database', 'database', '--table', 'table'], true)
          command.connector_issue(op)

          buf.string
        ensure
          $stdout = backup
          $stderr = stderr_backup
        end
      end

      describe 'queueing job' do
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

        context 'table is exist' do
          it 'should not create table' do
            client.should_receive(:create_log_table).and_return { raise TreasureData::AlreadyExistsError }

            subject
            expect(stderr_io.string).to be_empty
          end
        end

        context 'table is not exist' do
          it 'should not create table' do
            client.should_receive(:create_log_table).with('database', 'table')

            subject
            expect(stderr_io.string).to include "'database.table' is created."
          end
        end
      end
    end
  end
end
