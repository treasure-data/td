require 'spec_helper'
require 'td/helpers'
require 'open3'

module TreasureData

  describe 'format_with_delimiter' do
    it "delimits the number with ',' by default" do
      expect(Helpers.format_with_delimiter(0)).to eq("0")
      expect(Helpers.format_with_delimiter(10)).to eq("10")
      expect(Helpers.format_with_delimiter(100)).to eq("100")
      expect(Helpers.format_with_delimiter(1000)).to eq("1,000")
      expect(Helpers.format_with_delimiter(10000)).to eq("10,000")
      expect(Helpers.format_with_delimiter(100000)).to eq("100,000")
      expect(Helpers.format_with_delimiter(1000000)).to eq("1,000,000")
    end
  end

  describe 'on_64bit_os?' do

    def with_env(name, var)
      backup, ENV[name] = ENV[name], var
      begin
        yield
      ensure
        ENV[name] = backup
      end
    end

    it 'returns true for windows when PROCESSOR_ARCHITECTURE=amd64' do
      allow(Helpers).to receive(:on_windows?) {true}
      with_env('PROCESSOR_ARCHITECTURE', 'amd64') {
        expect(Helpers.on_64bit_os?).to be(true)
      }
    end

    it 'returns true for windows when PROCESSOR_ARCHITECTURE=x86 and PROCESSOR_ARCHITEW6432 is set' do
      allow(Helpers).to receive(:on_windows?) {true}
      with_env('PROCESSOR_ARCHITECTURE', 'x86') {
        with_env('PROCESSOR_ARCHITEW6432', '') {
          expect(Helpers.on_64bit_os?).to be(true)
        }
      }
    end

    it 'returns false for windows when PROCESSOR_ARCHITECTURE=x86 and PROCESSOR_ARCHITEW6432 is not set' do
      allow(Helpers).to receive(:on_windows?) {true}
      with_env('PROCESSOR_ARCHITECTURE', 'x86') {
        with_env('PROCESSOR_ARCHITEW6432', nil) {
          expect(Helpers.on_64bit_os?).to be(false)
        }
      }
    end

    it 'returns true for non-windows when uname -m prints x86_64' do
      allow(Helpers).to receive(:on_windows?) {false}
      allow(Open3).to receive(:capture2).with('uname', '-m') {['x86_64', double(:success? => true)]}
      expect(Helpers.on_64bit_os?).to be(true)
      expect(Open3).to have_received(:capture2).with('uname', '-m')
    end

    it 'returns false for non-windows when uname -m prints i686' do
      allow(Helpers).to receive(:on_windows?) {false}
      allow(Open3).to receive(:capture2).with('uname', '-m') {['i686', double(:success? => true)]}
      expect(Helpers.on_64bit_os?).to be(false)
      expect(Open3).to have_received(:capture2).with('uname', '-m')
    end

  end

end
