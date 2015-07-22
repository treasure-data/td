require 'spec_helper'
require 'td/command/common'
require 'td/command/list'
require 'td/command/connector'

module TreasureData::Command
  describe 'connector commands' do
    let :command do
      Class.new { include TreasureData::Command }.new
    end
    let(:bulk_load_yaml) { File.join("spec", "td", "fixture", "bulk_load.yml") }

    describe '#connector_guess' do

      describe 'guess plugins' do
        let(:guess_plugins) { %w(json query_string) }
        let(:stdout_io) { StringIO.new }
        let(:stderr_io) { StringIO.new }
        let(:option) {
          List::CommandParser.new("connector:guess", ["config"], [], nil, [in_file.path, '-o', out_file.path, '--guess', guess_plugins.join(',')], true)
        }
        let(:response) {
          {'config' => {'in' => {}, 'out' => {}}}
        }
        let(:client)   { double(:client) }
        let(:in_file)  { Tempfile.new('in.yml').tap{|f| f.close } }
        let(:out_file) { Tempfile.new('out.yml').tap{|f| f.close } }

        before do
          command.stub(:get_client).and_return(client)
        end

        let(:config) {
          {
            'in' => {'type' => 's3'}
          }
        }
        let(:expect_config) {
          config.merge('out' => {}, 'exec' => {'guess_plugins' => guess_plugins}, 'filters' => [])
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

      describe 'output config' do
        let(:stdout_io) { StringIO.new }
        let(:stderr_io) { StringIO.new }
        let(:out_file)  { Tempfile.new('out.yml') }
        let(:bulk_load_yaml) { File.join("spec", "td", "fixture", "bulk_load.yml") }
        let(:option) {
          List::CommandParser.new("connector:guess", ['config'], %w(access-id access-secret source out), nil, [bulk_load_yaml, '-o', out_file.path], true)
        }
        let(:response) {
          {'config' => {'in' => {}, 'out' => {}}}
        }
        let(:client) {
          double(:client, bulk_load_guess: response)
        }

        before do
          stdout = $stdout.dup
          stderr = $stderr.dup

          begin
            $stdout = stdout_io
            $stderr = stderr_io

            command.stub(:get_client).and_return(client)
            command.connector_guess(option)
          ensure
            $stdout = stdout
            $stderr = stderr
          end
        end

        it 'output yaml has [in, out] key' do
          expect(YAML.load_file(out_file.path).keys).to eq(%w(in out))
        end
      end
    end

    describe '#connector_preview' do
      subject do
        backup = $stdout.dup
        buf = StringIO.new

        begin
          $stdout = buf

          op = List::CommandParser.new("connector:preview", ["config"], [], nil, [bulk_load_yaml], true)
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

    describe '#connector_run' do
      include_context 'quiet_out'

      let(:client) { double(:client) }
      let(:job_name) { 'job_1' }

      before do
        command.stub(:get_client).and_return(client)
        client.stub(:database)
      end

      context 'with scheduled_time' do
        let(:scheduled_time) { Time.now + 60 }
        let(:option) {
          List::CommandParser.new('connector:run', ['name'], ['time'], nil, [job_name, scheduled_time.strftime("%Y-%m-%d %H:%M:%S")], true)
        }

        it 'client call with unix time' do
          client.should_receive(:bulk_load_run).with(job_name, scheduled_time.to_i).and_return(123)

          command.connector_run(option)
        end
      end

      context 'without scheduled_time' do
        let(:option) {
          List::CommandParser.new('connector:run', ['name'], ['time'], nil, [job_name], true)
        }
        let(:current_time) { Time.now }

        it 'client call with unix time' do
          client.should_receive(:bulk_load_run).with(job_name, current_time.to_i).and_return(123)
          command.stub(:current_time).and_return(current_time.to_i)

          command.connector_run(option)
        end
      end
    end

    describe 'connector history' do
      include_context 'quiet_out'

      let(:name) { 'connector_test' }

      subject do
        op = List::CommandParser.new("connector:history", ["name"], [], nil, [name], true)
        command.connector_history(op)
      end

      before do
        client = double(:client)
        client.stub(:bulk_load_history).with(name).and_return(history)
        command.stub(:get_client).and_return(client)
      end

      context 'history is empty' do
        let(:history) { [] }

        it { expect { subject }.not_to raise_error }
      end

      context 'history in not empty' do
        let(:history) { [column] }
        let(:column) {
          # TODO set real value
          {
            'job_id'   => '',
            'status'   => '',
            'records'  => '',
            'database' => {'name' => ''},
            'table'    => {'name' => ''},
            'priority' =>  ''
          }
        }

        context 'job is queueing' do
          before do
            column['start_at'] = nil
            column['end_at']   = nil
          end

          it { expect { subject }.not_to raise_error }
        end

        context 'job is running' do
          before do
            column['start_at'] = Time.now.to_i
            column['end_at']   = nil
          end

          it { expect { subject }.not_to raise_error }
        end

        context 'jobi is finished' do
          before do
            column['start_at'] = Time.now.to_i
            column['end_at']   = (Time.now + 60).to_i
          end

          it { expect { subject }.not_to raise_error }
        end
      end
    end

    describe '#connector_list' do
      let(:stdout_io) { StringIO.new }
      let(:stderr_io) { StringIO.new }
      let(:option) {
        List::CommandParser.new("connector:list", [], [], nil, [], true)
      }
      let(:response) {
        [{
          'name'     => 'daily_mysql_import',
          'cron'     => '10 0 * * *',
          'timezone' => 'UTC',
          'delay'    => 0,
          'database' => 'td_sample_db',
          'table'    => 'td_sample_table',
          'config'   => {'type' => 'mysql'},
        }]
      }
      let(:client) {
        double(:client, bulk_load_list: response)
      }

      before do
        stdout = $stdout.dup
        stderr = $stderr.dup

        begin
          $stdout = stdout_io
          $stderr = stderr_io

          command.stub(:get_client).and_return(client)
          command.connector_list(option)
        ensure
          $stdout = stdout
          $stderr = stderr
        end
      end

      it 'show list use table format' do
        expect(stdout_io.string).to include <<-EOL
| daily_mysql_import | 10 0 * * * | UTC      | 0     | td_sample_db | td_sample_table | {"type"=>"mysql"} |
        EOL
      end
    end
  end
end
