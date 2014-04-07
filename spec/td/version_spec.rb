require 'spec_helper'

module TreasureData::Command
  describe '--version' do
    it "shows version" do
      stderr, stdout = execute_td("--version")
      stderr.should == ""
      stdout.should == <<-STDOUT
#{TreasureData::TOOLBELT_VERSION}
STDOUT
    end
  end
end
