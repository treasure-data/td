require 'spec_helper'
require 'td/command/common'
require 'td/command/import'

module TreasureData::Command
  describe 'import commands' do
    describe CommandExecutor do
      it 'executes command' do
        CommandExecutor.new(['echo'], nil).execute.exitstatus.should == 0
      end

      it 'executes command' do
        expect {
          CommandExecutor.new(['exit', '1'], nil).execute
        }.to raise_error BulkImportExecutionError, /td-bulk-import\.log/
      end

      it 'terminates process on timeout' do
        CommandExecutor.new(['sleep', '1'], 2).execute
        expect {
          CommandExecutor.new(['sleep', '2'], 1).execute
        }.to raise_error BulkImportExecutionError, /timed out/
      end
    end
  end
end
