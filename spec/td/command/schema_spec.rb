require 'spec_helper'
require 'td/command/common'
require 'td/config'
require 'td/command/list'
require 'td/command/schema'
require 'td/client/model'
require 'time'

describe TreasureData::Command do
  let(:client){ double('client') }
  let(:table){ double('table', schema: schema) }
  let(:db_name){ "database" }
  let(:table_name){ "table" }
  let(:command) { Class.new { include TreasureData::Command }.new }
  let(:columns){ ["foo:int", "BAR\u3070\u30FC:string@bar", "baz:baz!:array<double>@baz"] }
  let(:schema){ TreasureData::Schema.parse(columns) }
  let(:stdout){ StringIO.new }
  before do
    allow(command).to receive(:get_client).and_return(client)
    allow(command).to receive(:get_table).with(client, db_name, table_name).and_return(table)
    allow(table).to receive(:get_table).with(client, db_name, table_name).and_return(table)
    allow($stdout).to receive(:puts){|arg| stdout.puts(*arg) }
  end

  describe 'schema_show' do
    let(:op){ double('op', cmd_parse: [db_name, table_name]) }
    it 'puts $stdout the schema' do
      command.schema_show(op)
      expect(stdout.string).to eq <<eom
database.table (
  foo:int
  BAR\u3070\u30FC:string@bar
  baz:baz!:array<double>@baz
)
eom
    end
  end

  describe 'schema_set' do
    let(:op){ double('op', cmd_parse: [db_name, table_name, *columns]) }
    it 'calls client.update_schema' do
      allow($stderr).to receive(:puts)
      expect(client).to receive(:update_schema) do |x, y, z|
        expect(x).to eq db_name
        expect(y).to eq table_name
        expect(z.to_json).to eq schema.to_json
      end
      command.schema_set(op)
    end
  end

  describe 'schema_add' do
    let(:columns2){ ["foo2:int", "BAR\u30702:string@bar2", "baz:baz!2:array<double>@baz2"] }
    let(:schema2){ TreasureData::Schema.parse(columns+columns2) }
    let(:op){ double('op', cmd_parse: [db_name, table_name, *columns2]) }
    it 'calls client.update_schema' do
      allow($stderr).to receive(:puts)
      expect(client).to receive(:update_schema) do |x, y, z|
        expect(x).to eq db_name
        expect(y).to eq table_name
        expect(z.to_json).to eq schema2.to_json
      end
      command.schema_add(op)
    end
  end

  describe 'schema_remove' do
    let(:op){ double('op', cmd_parse: [db_name, table_name, "foo"]) }
    let(:columns2){ ["BAR\u3070\u30FC:string@bar", "baz:baz!:array<double>@baz"] }
    let(:schema2){ TreasureData::Schema.parse(columns2) }
    it 'calls client.update_schema' do
      allow($stderr).to receive(:puts)
      expect(client).to receive(:update_schema) do |x, y, z|
        expect(x).to eq db_name
        expect(y).to eq table_name
        expect(z.to_json).to eq schema.to_json
      end
      command.schema_remove(op)
    end
  end
end
