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
          allow(command).to receive(:get_client).and_return(client)
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
          allow(command).to receive(:prepare_bulkload_job_config).and_return(config)
        end

        it 'guess_plugins passed td-client' do
          expect(client).to receive(:bulk_load_guess).with({config: expect_config}).and_return({})

          command.connector_guess(option)
        end
      end

      describe 'output config' do
        let(:out_file)  { Tempfile.new('out.yml').tap(&:close)  }
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
            allow(command).to receive(:get_client).and_return(client)
            command.connector_guess(option)
        end

        it 'output yaml has [in, out] key' do
          expect(YAML.load_file(out_file.path).keys).to eq(%w(in out))
        end
      end
    end

    describe '#connector_preview' do
      subject do
        op = List::CommandParser.new("connector:preview", ["config"], [], nil, [bulk_load_yaml], true)
        command.connector_preview(op)

        stdout_io.string
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
        allow(command).to receive(:get_client).and_return(client)
      end

      it 'should include too_long_column_name without truncated' do
        too_long_column_name = preview_result["schema"][0]["name"]
        expect(subject).to include "#{too_long_column_name}:string"
      end
    end

    describe '#connector_issue' do
      subject do
          command.connector_issue(option)

          stdout_io.string
      end

      describe 'queueing job' do
        let(:option) {
          List::CommandParser.new("connector:issue", ["config"], ['database', 'table'], nil, [File.join("spec", "td", "fixture", "bulk_load.yml"), '--database', 'database', '--table', 'table'], true)
        }

        before do
          client = double(:client, bulk_load_issue: 1234)
          allow(command).to receive(:get_client).and_return(client)
          allow(command).to receive(:create_database_and_table_if_not_exist)
        end

        it 'should include too_long_column_name without truncated' do
          expect(subject).to include "Job 1234 is queued."
        end
      end

      describe 'distination table' do
        let(:client) { double(:client, bulk_load_issue: 1234) }

        before do
          allow(command).to receive(:get_client).and_return(client)
          allow(client).to receive(:database)
        end

        context 'set auto crate table option' do
          let(:option) {
            List::CommandParser.new("connector:issue", ["config"], ['database', 'table'], nil, [File.join("spec", "td", "fixture", "bulk_load.yml"), '--database', 'database', '--table', 'table', '--auto-create-table'], true)
          }

          it 'call create_database_and_table_if_not_exist' do
            expect(command).to receive(:create_database_and_table_if_not_exist)

            subject
          end
        end

        context 'not set auto crate table option' do
          let(:option) {
            List::CommandParser.new("connector:issue", ["config"], ['database', 'table'], nil, [File.join("spec", "td", "fixture", "bulk_load.yml"), '--database', 'database', '--table', 'table'], true)
          }

          it 'call create_database_and_table_if_not_exist' do
            expect(command).not_to receive(:create_database_and_table_if_not_exist)

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
        allow(command).to receive(:get_client).and_return(client)
        allow(client).to receive(:database)
      end

      context 'with scheduled_time' do
        let(:scheduled_time) { Time.now + 60 }
        let(:option) {
          List::CommandParser.new('connector:run', ['name'], ['time'], nil, [job_name, scheduled_time.strftime("%Y-%m-%d %H:%M:%S")], true)
        }

        it 'client call with unix time' do
          expect(client).to receive(:bulk_load_run).with(job_name, scheduled_time.to_i).and_return(123)

          command.connector_run(option)
        end
      end

      context 'without scheduled_time' do
        let(:option) {
          List::CommandParser.new('connector:run', ['name'], ['time'], nil, [job_name], true)
        }
        let(:current_time) { Time.now }

        it 'client call with unix time' do
          expect(client).to receive(:bulk_load_run).with(job_name, current_time.to_i).and_return(123)
          allow(command).to receive(:current_time).and_return(current_time.to_i)

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
        allow(client).to receive(:bulk_load_history).with(name).and_return(history)
        allow(command).to receive(:get_client).and_return(client)
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
        allow(command).to receive(:get_client).and_return(client)
        command.connector_list(option)
      end

      it 'show list use table format' do
        expect(stdout_io.string).to include <<-EOL
| daily_mysql_import | 10 0 * * * | UTC      | 0     | td_sample_db | td_sample_table |
        EOL
      end
    end

    describe '#connector_create' do
      let(:name)     { 'daily_mysql_import' }
      let(:cron)     { '10 0 * * *' }
      let(:database) { 'td_sample_db' }
      let(:table)    { 'td_sample_table' }
      let(:config_file) {
        Tempfile.new('config.json').tap {|tf|
          tf.puts({}.to_json)
          tf.close
        }
      }
      let(:option) {
        List::CommandParser.new("connector:create", %w(name cron database table config_file), [], nil, [name, cron, database, table, config_file.path], true)
      }
      let(:response) {
        {'name' => name, 'cron' => cron, 'timezone' => 'UTC', 'delay' => 0, 'database' => database, 'table' => table, 'config' => ''}
      }
      let(:client) {
        double(:client, bulk_load_create: response)
      }

      before do
        allow(command).to receive(:get_table)
        allow(command).to receive(:get_client).and_return(client)
        command.connector_create(option)
      end

      it 'show create result' do
        expect(stdout_io.string).to include name
        expect(stdout_io.string).to include cron
        expect(stdout_io.string).to include database
        expect(stdout_io.string).to include table
      end
    end

    describe '#connector_update' do
      let(:name)     { 'daily_mysql_import' }
      let(:name2)     { 'daily_mysql_import2' }
      let(:cron)     { '10 0 * * *' }
      let(:cron2)    { '20 0 * * *' }
      let(:database) { 'td_sample_db' }
      let(:table)    { 'td_sample_table' }
      let(:config_file) {
        Tempfile.new('config.yml').tap {|tf|
          tf.puts({"foo" => "bar"}.to_yaml)
          tf.close
        }
      }
      let(:config) {
        h = YAML.load_file(config_file.path)
        TreasureData::ConnectorConfigNormalizer.new(h).normalized_config
      }
      let(:config_diff_file) {
        Tempfile.new('config_diff.yml').tap {|tf|
          tf.puts({"hoge" => "fuga"}.to_yaml)
          tf.close
        }
      }
      let(:config_diff) {
        h = YAML.load_file(config_diff_file.path)
        TreasureData::ConnectorConfigNormalizer.new(h).normalized_config
      }
      let(:option) {
        List::CommandParser.new("connector:update", %w(name), %w(config_file), nil, argv, true)
      }
      let(:response) {
        {'name' => name, 'cron' => cron, 'timezone' => 'UTC', 'delay' => 0, 'database' => database, 'table' => table,
          'config' => config, 'config_diff' => config_diff}
      }
      let(:client) { double(:client) }

      before do
        allow(command).to receive(:get_client).and_return(client)
        allow(client).to receive(:bulk_load_update) do |name, settings|
          r = response.merge('name' => name)
          settings.each do |key, value|
            value = nil if key == 'cron' && value.empty?
            r[key.to_s] = value
          end
          r
        end
      end

      context 'with new_name' do
        let (:argv){ ['--newname', name2, name] }
        it 'show update result' do
          expect{command.connector_update(option)}.not_to raise_error(SystemExit)
          expect(stdout_io.string).to include name2
          expect(stdout_io.string).to include cron
          expect(stdout_io.string).to include database
          expect(stdout_io.string).to include table
          expect(YAML.load(stdout_io.string[/^Config\n---\n(.*?\n)\n/m, 1])).to eq(config)
          expect(YAML.load(stdout_io.string[/^Config Diff\n---\n(.*?\n)\Z/m, 1])).to eq(config_diff)
        end
      end

      context 'with config' do
        let (:argv){ [name, config_file.path] }
        it 'show update result' do
          expect{command.connector_update(option)}.not_to raise_error(SystemExit)
          expect(stdout_io.string).to include name
          expect(stdout_io.string).to include cron
          expect(stdout_io.string).to include database
          expect(stdout_io.string).to include table
          expect(YAML.load(stdout_io.string[/^Config\n---\n(.*?\n)\n/m, 1])).to eq(config)
          expect(YAML.load(stdout_io.string[/^Config Diff\n---\n(.*?\n)\Z/m, 1])).to eq(config_diff)
        end
      end

      context 'with config_diff' do
        let(:argv){ ['--config-diff', config_diff_file.path, name] }
        it 'show update result' do
          expect{command.connector_update(option)}.not_to raise_error(SystemExit)
          expect(stdout_io.string).to include name
          expect(stdout_io.string).to include cron
          expect(stdout_io.string).to include database
          expect(stdout_io.string).to include table
          expect(YAML.load(stdout_io.string[/^Config\n---\n(.*?\n)\n/m, 1])).to eq(config)
          expect(YAML.load(stdout_io.string[/^Config Diff\n---\n(.*?\n)\Z/m, 1])).to eq(config_diff)
        end
      end

      context 'with schedule' do
        let(:argv) { ['--schedule', cron2, name] }
        it 'can update cron' do
          expect{command.connector_update(option)}.not_to raise_error(SystemExit)
          expect(stdout_io.string).to include name
          expect(stdout_io.string).to include cron2
          expect(stdout_io.string).to include database
          expect(stdout_io.string).to include table
          expect(YAML.load(stdout_io.string[/^Config\n---\n(.*?\n)\n/m, 1])).to eq(config)
          expect(YAML.load(stdout_io.string[/^Config Diff\n---\n(.*?\n)\Z/m, 1])).to eq(config_diff)
        end
      end

      context 'with empty schedule' do
        let(:argv) { [name, '--schedule'] }
        it 'can update cron' do
          expect{command.connector_update(option)}.not_to raise_error(SystemExit)
          expect(stdout_io.string).to include name
          expect(stdout_io.string).to include "Cron     : \n"
          expect(stdout_io.string).to include database
          expect(stdout_io.string).to include table
          expect(YAML.load(stdout_io.string[/^Config\n---\n(.*?\n)\n/m, 1])).to eq(config)
          expect(YAML.load(stdout_io.string[/^Config Diff\n---\n(.*?\n)\Z/m, 1])).to eq(config_diff)
        end
      end

      context 'nothing to update' do
        let (:argv) { [name] }
        it 'show update result' do
          expect{command.connector_update(option)}.to raise_error(SystemExit)
          expect(stdout_io.string).to include 'Error: nothing to update'
        end
      end
    end
  end
end
