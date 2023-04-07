# encoding: utf-8

require 'spec_helper'
require "tempfile"
require 'td/command/common'
require 'td/command/account'
require 'td/command/list'

module TreasureData::Command
  describe 'account command' do
    let :command do
      Class.new { include TreasureData::Command }.new
    end
    let(:client) { double(:client) }
    let(:conf_file) { Tempfile.new("td.conf").tap {|s| s.close } }
    let(:conf_expect) {
"""[account]
  user = test@email.com
  apikey = 1/xxx
"""
    }

    before do
      TreasureData::Config.path = conf_file.path
      $stdout.puts "conf_file.path " + conf_file.path
    end

    it 'is called without any option' do
      expect(STDIN).to receive(:gets).and_return('test@email.com')
      expect(STDIN).to receive(:gets).and_return('password')
      expect(TreasureData::Client).to receive(:authenticate).and_return(client)
      expect(client).to receive(:apikey).and_return("1/xxx")
      op = List::CommandParser.new("account", %w[], %w[], false, [], true)
      command.account(op)
      expect(File.read(conf_file.path)).to eq(conf_expect)
    end

    it 'is called with -f option' do
      expect(STDIN).to receive(:gets).and_return('password')
      expect(TreasureData::Client).to receive(:authenticate).and_return(client)
      expect(client).to receive(:apikey).and_return("1/xxx")
      op = List::CommandParser.new("account", %w[-f], %w[], false, ['test@email.com'], true)
      command.account(op)
      expect(File.read(conf_file.path)).to eq(conf_expect)
    end

    it 'is called with username password mismatched' do
      expect(STDIN).to receive(:gets).and_return('password').thrice
      expect(TreasureData::Client).to receive(:authenticate).thrice.and_raise(TreasureData::AuthError)
      expect(STDERR).to receive(:puts).with('User name or password mismatched.').thrice
      op = List::CommandParser.new("account", %w[-f], %w[], false, ['test@email.com'], true)
      command.account(op)
      expect(File.read(conf_file.path)).to eq("")
    end
  end
end