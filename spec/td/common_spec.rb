require 'spec_helper'
require 'td/command/common'

module TreasureData::Command
  describe 'humanize_bytesize' do
    describe 'for values < 1024' do
      values = [0, 1, 10, 1023]
      values.each {|v|
        it "uses B as label and has no suffix (#{v})" do
          TreasureData::Command::humanize_bytesize(v, 1).should == "#{v} B"
        end
      }
    end

    describe 'for 1024' do
      it 'uses kB and does not have a suffix' do
        TreasureData::Command::humanize_bytesize(1024).should == "1 kB"
      end
    end
    describe 'for values between 1025 and (1024^2 - 1)' do
      base = 1024
      values = [
        [base + 1, "1.0"],
        [base + 2, "1.0"],
        [base * 1024 - 1, "1023.9"]
      ]
      values.each {|val, exp|
        it "uses kB as label and has a suffix (#{val})" do
          result = TreasureData::Command::humanize_bytesize(val, 1)
          expect(result).to eq("#{exp} kB")
        end
      }
    end

    describe 'for 1024^2' do
      it 'uses MB and does not have a suffix' do
        TreasureData::Command::humanize_bytesize(1024 ** 2).should == "1 MB"
      end
    end
    describe 'for values between (1024^2 + 1) and (1024^3 - 1)' do
      base = 1024 ** 2
      values = [
        [base + 1, "1.0"],
        [base + 2, "1.0"],
        [base * 1024 - 1, "1023.9"]
      ]
      values.each {|val, exp|
        it "uses MB as label and has a suffix (#{val})" do
          result = TreasureData::Command::humanize_bytesize(val, 1)
          expect(result).to eq("#{exp} MB")
        end
      }
    end

    describe 'for 1024^3' do
      it 'uses GB and does not have a suffix' do
        TreasureData::Command::humanize_bytesize(1024 ** 3).should == "1 GB"
      end
    end
    describe 'for values between (1024^3 + 1) and (1024^4 - 1)' do
      base = 1024 ** 3
      values = [
        [base + 1, "1.0"],
        [base + 2, "1.0"],
        [base * 1024 - 1, "1023.9"]
      ]
      values.each {|val, exp|
        it "uses GB as label and has a suffix (#{val})" do
          result = TreasureData::Command::humanize_bytesize(val, 1)
          expect(result).to eq("#{exp} GB")
        end
      }
    end

    describe 'for 1024^4' do
      it 'uses TB and does not have a suffix' do
        TreasureData::Command::humanize_bytesize(1024 ** 4).should == "1 TB"
      end
    end
    describe 'for values between (1024^4 + 1) and (1024^5 - 1)' do
      base = 1024 ** 4
      values = [
        [base + 1, "1.0"],
        [base + 2, "1.0"],
        [base * 1024 - 1, "1023.9"]
      ]
      values.each {|val, exp|
        it "uses TB as label and has a suffix (#{val})" do
          result = TreasureData::Command::humanize_bytesize(val, 1)
          expect(result).to eq("#{exp} TB")
        end
      }
    end

    describe 'for 1024^5' do
      it 'uses TB and does not have a suffix' do
        TreasureData::Command::humanize_bytesize(1024 ** 5).should == "1024 TB"
      end
    end
    describe 'for values between (1024^5 + 1) and (1024^6 - 1)' do
      base = 1024 ** 5
      values = [
        [base + 1, "1024.0"],
        [base + 2, "1024.0"],
        [base * 1024 - 1, "1048575.9"]
      ]
      values.each {|val, exp|
        it "uses TB as label and has a suffix (#{val})" do
          result = TreasureData::Command::humanize_bytesize(val, 1)
          expect(result).to eq("#{exp} TB")
        end
      }
    end

    describe 'shows 1 digit' do
      it 'without second function argument' do
        values = [1024 + 1024 / 2, "1.5"]
        val, exp = values
        result = TreasureData::Command::humanize_bytesize(val)
        expect(result).to eq("#{exp} kB")
      end
    end
    describe 'shows the correct number of digits specified by the second argument' do
      (0...5).each {|i|
        it "when = #{i}" do
          val = 1024 + 1024 / 2
          if i == 0
            exp = 1.to_s
          else
            exp = sprintf "%.*f", i, 1.5
          end
          result = TreasureData::Command::humanize_bytesize(val, i)
          expect(result).to eq("#{exp} kB")
        end
      }
    end
  end
end
