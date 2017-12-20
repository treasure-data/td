require 'spec_helper'
require 'td/command/common'
require 'td/config'
require 'td/command/list'
require 'td/command/table'
require 'td/client/model'

module TreasureData::Command

  describe 'table command' do
    describe 'table_list' do
      it 'lists tables in a database' do
        client = Object.new

        db = TreasureData::Database.new(client, 'full_access_db', nil, 1000, Time.now.to_i, Time.now.to_i, nil, 'full_access')

        create_tables = lambda {|db_name|
          (1..6).map {|i|
            schema = TreasureData::Schema.new.from_json(JSON.parse('[]'))
            TreasureData::Table.new(client, db_name, db_name + "_table_#{i}", 'log', schema, 500, Time.now.to_i, Time.now.to_i, 0, nil, nil, nil)
          }
        }
        db_tables = create_tables.call(db.name)

        allow(client).to receive(:tables).with(db.name).and_return(db_tables)

        command = Class.new { include TreasureData::Command }.new
        allow(command).to receive(:get_client).and_return(client)
        allow(command).to receive(:get_database).and_return(db)

        op = List::CommandParser.new('table:list', %w[], %w[db], false, %w(full_access_db), true)
        expect {
          command.table_list(op)
        }.to_not raise_exception
      end

      it 'lists all tables in all databases' do
        client = Object.new

        qo_db = TreasureData::Database.new(client, 'query_only_db', nil, 2000, Time.now.to_i, Time.now.to_i, nil, 'query_only')
        fa_db = TreasureData::Database.new(client, 'full_access_db', nil, 3000, Time.now.to_i, Time.now.to_i, nil, 'full_access')
        own_db = TreasureData::Database.new(client, 'owner_db', nil, 4000, Time.now.to_i, Time.now.to_i, nil, 'owner')

        create_tables = lambda {|db_name|
          (1..6).map {|i|
            schema = TreasureData::Schema.new.from_json(JSON.parse('[]'))
            TreasureData::Table.new(client, db_name, db_name + "_table_#{i}", 'log', schema, 500, Time.now.to_i, Time.now.to_i, 0, nil, nil, nil)
          }
        }
        qo_db_tables = create_tables.call(qo_db.name)
        fa_db_tables = create_tables.call(fa_db.name)
        own_db_tables = create_tables.call(own_db.name)

        allow(client).to receive(:databases).and_return([qo_db, fa_db, own_db])

        allow(client).to receive(:tables).with(qo_db.name).and_return(qo_db_tables)
        allow(client).to receive(:tables).with(fa_db.name).and_return(fa_db_tables)
        allow(client).to receive(:tables).with(own_db.name).and_return(own_db_tables)

        command = Class.new { include TreasureData::Command }.new
        allow(command).to receive(:get_client).and_return(client)

        op = List::CommandParser.new('table:list', %w[], %w[db], false, %w(), true)
        expect {
          command.table_list(op)
        }.to_not raise_exception
      end

      it 'avoids listing tables of an \'import_only\' database' do
        client = Object.new

        db = TreasureData::Database.new(client, 'import_only_db', nil, 1234, Time.now.to_i, Time.now.to_i, nil, 'import_only')

        command = Class.new { include TreasureData::Command }.new
        allow(command).to receive(:get_client).and_return(client)
        allow(command).to receive(:get_database).and_return(db)

        op = List::CommandParser.new('table:list', %w[], %w[db], false, %w(import_only_db), true)
        expect {
          command.table_list(op)
        }.to_not raise_exception
      end

      it 'avoids listing tables of the \'import_only\' databases in the list' do
        client = Object.new

        io_db = TreasureData::Database.new(client, 'import_only_db', nil, 1000, Time.now.to_i, Time.now.to_i, nil, 'import_only')
        qo_db = TreasureData::Database.new(client, 'query_only_db', nil, 2000, Time.now.to_i, Time.now.to_i, nil, 'query_only')
        fa_db = TreasureData::Database.new(client, 'full_access_db', nil, 3000, Time.now.to_i, Time.now.to_i, nil, 'full_access')
        own_db = TreasureData::Database.new(client, 'owner_db', nil, 4000, Time.now.to_i, Time.now.to_i, nil, 'owner')

        create_tables = lambda {|db_name|
          (1..6).map {|i|
            schema = TreasureData::Schema.new.from_json(JSON.parse('[]'))
            TreasureData::Table.new(client, db_name, db_name + "_table_#{i}", 'log', schema, 500, Time.now.to_i, Time.now.to_i, 0, nil, nil, nil)
          }
        }
        qo_db_tables = create_tables.call(qo_db.name)
        fa_db_tables = create_tables.call(fa_db.name)
        own_db_tables = create_tables.call(own_db.name)

        allow(client).to receive(:databases).and_return([io_db, qo_db, fa_db, own_db])

        allow(client).to receive(:tables).with(io_db.name).and_raise("not permitted")
        allow(client).to receive(:tables).with(qo_db.name).and_return(qo_db_tables)
        allow(client).to receive(:tables).with(fa_db.name).and_return(fa_db_tables)
        allow(client).to receive(:tables).with(own_db.name).and_return(own_db_tables)

        command = Class.new { include TreasureData::Command }.new
        allow(command).to receive(:get_client).and_return(client)

        op = List::CommandParser.new('table:list', %w[], %w[db], false, %w(), true)
        expect {
          command.table_list(op)
        }.to_not raise_exception
      end

      describe "number format" do
        let(:number_raw) { "1234567" }
        let(:number_format) { "1,234,567" }
        let(:client) { double('null object').as_null_object }
        let(:db) { TreasureData::Database.new(client, 'full_access_db', nil, 1000, Time.now.to_i, Time.now.to_i, nil, 'full_access') }
        let(:command) do
          command = Class.new { include TreasureData::Command }.new
          allow(command).to receive(:get_client).and_return(client)
          allow(command).to receive(:get_database).and_return(db)
          command
        end

        before do
          create_tables = lambda {|db_name|
            (1..6).map {|i|
              schema = TreasureData::Schema.new.from_json(JSON.parse('[]'))
              TreasureData::Table.new(client, db_name, db_name + "_table_#{i}", 'log', schema, number_raw, Time.now.to_i, Time.now.to_i, 0, nil, nil, nil)
            }
          }
          db_tables = create_tables.call(db.name)
          allow(client).to receive(:tables).with(db.name).and_return(db_tables)
        end

        subject do
          # command.table_list uses `puts` to display result
          # so temporary swapping $stdout with StringIO to fetch their output
          backup = $stdout.dup
          buf = StringIO.new
          op = List::CommandParser.new('table:list', [], %w[db], false, options + %w(full_access_db), true)
          begin
            $stdout = buf
            command.table_list(op)
            $stdout.rewind
            $stdout.read
          ensure
            $stdout = backup
          end
        end

        context "without --format" do
          let(:options) { [] }
          it { is_expected.to include(number_format) }
          it { is_expected.not_to include(number_raw) }
        end

        context "with --format table" do
          let(:options) { %w(--format table) }
          it { is_expected.to include(number_format) }
          it { is_expected.not_to include(number_raw) }
        end

        context "with --format csv" do
          let(:options) { %w(--format csv) }
          it { is_expected.not_to include(number_format) }
          it { is_expected.to include(number_raw) }
        end

        context "with --format tsv" do
          let(:options) { %w(--format tsv) }
          it { is_expected.not_to include(number_format) }
          it { is_expected.to include(number_raw) }
        end
      end
    end

    describe '#table_create' do
      let(:db_name)    { 'database' }
      let(:table_name) { 'table1' }
      let(:client)     { double('client') }
      let(:command)    { Class.new { include TreasureData::Command }.new }

      before do
        allow(command).to receive(:get_client) { client }
      end

      context 'normal' do
        let(:option) {
          List::CommandParser.new('table:create', %w(db_name table_name), [], false, [db_name, table_name], false)
        }

        it 'create table' do
          expect(client).to receive(:create_log_table).with(db_name, table_name, {})
          command.table_create(option)
        end
      end

      context 'with include_v' do
        let(:option) {
          List::CommandParser.new('table:create', %w(db_name table_name), [], false, [db_name, table_name, '--include-v'], false)
        }

        it 'raises Error' do
          expect{ command.table_create(option) }.to raise_error(SystemExit)
        end
      end

      context 'with include_v=true' do
        let(:option) {
          List::CommandParser.new('table:create', %w(db_name table_name), [], false, [db_name, table_name, '--include-v=true'], false)
        }

        it 'create table' do
          expect(client).to receive(:create_log_table).with(db_name, table_name, {include_v: true})
          expect{ command.table_create(option) }.not_to raise_error
        end
      end

      context 'with include_v=false' do
        let(:option) {
          List::CommandParser.new('table:create', %w(db_name table_name), [], false, [db_name, table_name, '--include-v=false'], false)
        }

        it 'create table' do
          expect(client).to receive(:create_log_table).with(db_name, table_name, {include_v: false})
          expect{ command.table_create(option) }.not_to raise_error
        end
      end

      context 'with detect_schema' do
        let(:option) {
          List::CommandParser.new('table:create', %w(db_name table_name), [], false, [db_name, table_name, '--detect-schema'], false)
        }

        it 'raises Error' do
          expect{ command.table_create(option) }.to raise_error(SystemExit)
        end
      end

      context 'with detect_schema=true' do
        let(:option) {
          List::CommandParser.new('table:create', %w(db_name table_name), [], false, [db_name, table_name, '--detect-schema=true'], false)
        }

        it 'create table' do
          expect(client).to receive(:create_log_table).with(db_name, table_name, {detect_schema: true})
          expect{ command.table_create(option) }.not_to raise_error
        end
      end

      context 'with detect_schema=false' do
        let(:option) {
          List::CommandParser.new('table:create', %w(db_name table_name), [], false, [db_name, table_name, '--detect-schema=false'], false)
        }

        it 'create table' do
          expect(client).to receive(:create_log_table).with(db_name, table_name, {detect_schema: false})
          expect{ command.table_create(option) }.not_to raise_error
        end
      end

      context 'with expire_days' do
        let(:option) {
          List::CommandParser.new('table:create', %w(db_name table_name), [], false, [db_name, table_name, '--expire-days'], false)
        }

        it 'raises Error' do
          expect{ command.table_create(option) }.to raise_error(SystemExit)
        end
      end

      context 'with expire_days=0' do
        let(:option) {
          List::CommandParser.new('table:create', %w(db_name table_name), [], false, [db_name, table_name, '--expire-days=0'], false)
        }

        it 'create table' do
          expect(client).to receive(:create_log_table).with(db_name, table_name, {expire_days: 0})
          expect{ command.table_create(option) }.not_to raise_error
        end
      end

      context 'with expire_days=365' do
        let(:option) {
          List::CommandParser.new('table:create', %w(db_name table_name), [], false, [db_name, table_name, '--expire-days=365'], false)
        }

        it 'create table' do
          expect(client).to receive(:create_log_table).with(db_name, table_name, {expire_days: 365})
          expect{ command.table_create(option) }.not_to raise_error
        end
      end
    end

    describe '#table_import' do
      let(:db_name)    { 'database' }
      let(:table_name) { 'table' }
      let(:client)     { double('client') }
      let(:command)    { Class.new { include TreasureData::Command }.new }

      describe 'auto create table' do
        before do
          allow(command).to receive(:get_client) { client }
          allow(command).to receive(:do_table_import)
        end

        context 'set --auto-create-table' do
          let(:option) {
            List::CommandParser.new('table:import', [], %w(db_name table_name path), false, [db_name, table_name, 'path', '--auto-create-table'], true)
          }

          it 'create table' do
            expect(command).to receive(:create_database_and_table_if_not_exist).with(client, db_name, table_name)
            command.table_import(option)
          end
        end

        context 'not set --auto-create-table' do
          let(:option) {
            List::CommandParser.new('table:import', [], %w(db_name table_name path), false, [db_name, table_name, 'path'], true)
          }

          it 'not create table' do
            expect(command).not_to receive(:create_database_and_table_if_not_exist)
            command.table_import(option)
          end
        end
      end

      describe 'time key' do
        before do
          allow(command).to receive(:get_client) { client }
          allow(command).to receive(:do_table_import)
        end
        let(:input_params) {{
          db_name: "database",
          table_name: "table",
          paths: ["path"]
        }}

        context 'should specify the custom time key' do
          ['-t', '--time-key'].each do |tk_option|
            let(:option) {
              List::CommandParser.new('table:import', [], %w(db_name table_name path), false, [db_name, table_name, 'path', tk_option, 'created_at'], true)
            }

            before do
              input_params[:time_key] = 'created_at'
            end

            it "with '#{tk_option}' option" do
              expect(command).to receive(:do_table_import).with(client, input_params)
              command.table_import(option)
            end
          end
        end

        context 'should use default time key \'time\'' do
          let(:option) {
            List::CommandParser.new('table:import', [], %w(db_name table_name path), false, [db_name, table_name, 'path'], true)
          }

          before do
            input_params[:time_key] = 'time'
          end

          it 'without \'-t / --time-key\' option' do
            expect(command).to receive(:do_table_import).with(client, input_params)
            command.table_import(option)
          end
        end
      end
    end

    describe '#table_rename' do
      include_context 'quiet_out'

      let(:db_name)         { 'database' }
      let(:from_table_name) { 'from_table' }
      let(:dest_table_name) { 'dest_table' }
      let(:client)          { double('client') }
      let(:database)        { double('database') }
      let(:command)         { Class.new { include TreasureData::Command }.new }
      let(:option) {
        List::CommandParser.new('table:rename', [], %w(db_name from_table_name dest_table_name), false, cmd_args, true)
      }

      before do
        allow(command).to receive(:get_client) { client }
        allow(command).to receive(:get_database).with(client, db_name).and_return(database)
      end

      context "from table isn't exists" do
        let(:cmd_args) { [db_name, from_table_name, dest_table_name] }

        before do
          allow(database).to receive(:table).with(from_table_name) { raise }
        end

        it "can't rename table" do
          expect(command).to receive(:exit).with(1) { raise CallSystemExitError }

          expect { command.table_rename(option) }.to raise_error
          expect(stderr_io.string).to     include(from_table_name)
          expect(stderr_io.string).not_to include(dest_table_name)
        end
      end

      context 'overwrite is false' do
        let(:cmd_args) { [db_name, from_table_name, dest_table_name] }

        before do
          allow(database).to receive(:table).with(from_table_name)
        end

        context "dest_table isn't exists" do
          before do
            allow(database).to receive(:table).with(dest_table_name) { raise }
          end

          it 'create dest table and rename table' do
            expect(client).to receive(:create_log_table).with(db_name, dest_table_name)
            expect(client).to receive(:swap_table).with(db_name, from_table_name, dest_table_name)
            expect(client).to receive(:delete_table).with(db_name, from_table_name)

            command.table_rename(option)

            expect(stderr_io.string).to include(from_table_name)
            expect(stderr_io.string).to include(dest_table_name)
          end
        end

        context "dest_tableis is exist" do
          before do
            allow(database).to receive(:table).with(dest_table_name)
          end

          it "can't rename table" do
            expect(command).to receive(:exit).with(1) { raise CallSystemExitError }

            expect { command.table_rename(option) }.to raise_error
            expect(stderr_io.string).not_to include(from_table_name)
            expect(stderr_io.string).to     include(dest_table_name)
          end
        end
      end

      context 'overwrite is true' do
        let(:cmd_args) { [db_name, from_table_name, dest_table_name, '--overwrite'] }

        before do
          allow(database).to receive(:table).with(from_table_name)
        end

        context "dest_table isn't exists" do
          before do
            allow(database).to receive(:table).with(dest_table_name) { raise }
          end

          it 'create dest table and rename table' do
            expect(client).to receive(:create_log_table).with(db_name, dest_table_name)
            expect(client).to receive(:swap_table).with(db_name, from_table_name, dest_table_name)
            expect(client).to receive(:delete_table).with(db_name, from_table_name)

            command.table_rename(option)
            expect(stderr_io.string).to include(from_table_name)
            expect(stderr_io.string).to include(dest_table_name)
          end
        end

        context "dest_tableis is exist" do
          before do
            allow(database).to receive(:table).with(dest_table_name)
          end

          it 'overwrite dest table' do
            expect(client).to receive(:swap_table).with(db_name, from_table_name, dest_table_name)
            expect(client).to receive(:delete_table).with(db_name, from_table_name)

            command.table_rename(option)
            expect(stderr_io.string).to include(from_table_name)
            expect(stderr_io.string).to include(dest_table_name)
          end
        end
      end
    end
  end
end
