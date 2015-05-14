require 'spec_helper'
require 'td/command/common'
require 'td/command/list'
require 'td/command/connector'

module TreasureData::Command
  describe 'connector commands' do
    describe '#connector_preview' do
      subject do
        backup = $stdout.dup
        buf = StringIO.new

        begin
          $stdout = buf

          TreasureData::Command::Runner.new.run ["connector:preview", tempfile]

          buf.string
        ensure
          $stdout = backup
        end
      end

      let(:tempfile) do
        File.join("spec", "td", "fixture", "bulk_load.yml")
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
        TreasureData::Client.any_instance.stub(:bulk_load_preview).and_return(preview_result)
      end

      it 'should include too_long_column_name without truncated' do
        too_long_column_name = preview_result["schema"][0]["name"]
        expect(subject).to include "#{too_long_column_name}:string"
      end
    end
  end
end
