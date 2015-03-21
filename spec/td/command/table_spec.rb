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
            TreasureData::Table.new(client, db_name, db_name + "_table_#{i}", 'log', schema, 500, Time.now.to_i, Time.now.to_i, 0, nil, nil, nil, nil, nil)
          }
        }
        db_tables = create_tables.call(db.name)

        client.stub(:tables).with(db.name).and_return(db_tables)

        command = Class.new { include TreasureData::Command }.new
        command.stub(:get_client).and_return(client)
        command.stub(:get_database).and_return(db)

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
            TreasureData::Table.new(client, db_name, db_name + "_table_#{i}", 'log', schema, 500, Time.now.to_i, Time.now.to_i, 0, nil, nil, nil, nil, nil)
          }
        }
        qo_db_tables = create_tables.call(qo_db.name)
        fa_db_tables = create_tables.call(fa_db.name)
        own_db_tables = create_tables.call(own_db.name)

        client.stub(:databases).and_return([qo_db, fa_db, own_db])

        client.stub(:tables).with(qo_db.name).and_return(qo_db_tables)
        client.stub(:tables).with(fa_db.name).and_return(fa_db_tables)
        client.stub(:tables).with(own_db.name).and_return(own_db_tables)

        command = Class.new { include TreasureData::Command }.new
        command.stub(:get_client).and_return(client)

        op = List::CommandParser.new('table:list', %w[], %w[db], false, %w(), true)
        expect {
          command.table_list(op)
        }.to_not raise_exception
      end

      it 'avoids listing tables of an \'import_only\' database' do
        client = Object.new

        db = TreasureData::Database.new(client, 'import_only_db', nil, 1234, Time.now.to_i, Time.now.to_i, nil, 'import_only')

        command = Class.new { include TreasureData::Command }.new
        command.stub(:get_client).and_return(client)
        command.stub(:get_database).and_return(db)

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
            TreasureData::Table.new(client, db_name, db_name + "_table_#{i}", 'log', schema, 500, Time.now.to_i, Time.now.to_i, 0, nil, nil, nil, nil, nil)
          }
        }
        qo_db_tables = create_tables.call(qo_db.name)
        fa_db_tables = create_tables.call(fa_db.name)
        own_db_tables = create_tables.call(own_db.name)

        client.stub(:databases).and_return([io_db, qo_db, fa_db, own_db])

        client.stub(:tables).with(io_db.name).and_raise("not permitted")
        client.stub(:tables).with(qo_db.name).and_return(qo_db_tables)
        client.stub(:tables).with(fa_db.name).and_return(fa_db_tables)
        client.stub(:tables).with(own_db.name).and_return(own_db_tables)

        command = Class.new { include TreasureData::Command }.new
        command.stub(:get_client).and_return(client)

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
          command.stub(:get_client).and_return(client)
          command.stub(:get_database).and_return(db)
          command
        end

        before do
          create_tables = lambda {|db_name|
            (1..6).map {|i|
              # NOTE: TreasureData::Helpers.format_with_delimiter uses `gsub!` to their argument
              #       the argument (in our case, `number_raw`) will be rewritten by them
              #       To avoid that behavior, pass `number_raw.dup` instead of `number_raw`
              schema = TreasureData::Schema.new.from_json(JSON.parse('[]'))
              TreasureData::Table.new(client, db_name, db_name + "_table_#{i}", 'log', schema, number_raw.dup, Time.now.to_i, Time.now.to_i, 0, nil, nil, nil, nil, nil)
            }
          }
          db_tables = create_tables.call(db.name)
          client.stub(:tables).with(db.name).and_return(db_tables)
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
          it { should include(number_format) }
          it { should_not include(number_raw) }
        end

        context "with --format table" do
          let(:options) { %w(--format table) }
          it { should include(number_format) }
          it { should_not include(number_raw) }
        end

        context "with --format csv" do
          let(:options) { %w(--format csv) }
          it { should_not include(number_format) }
          it { should include(number_raw) }
        end

        context "with --format tsv" do
          let(:options) { %w(--format tsv) }
          it { should_not include(number_format) }
          it { should include(number_raw) }
        end
      end
    end
  end
end
