module TreasureData
  module Helpers
    module_function

    def format_with_delimiter(number, delimiter = ',')
      num = number.to_s
      if formatted = num.gsub!(/(\d)(?=(?:\d{3})+(?!\d))/, "\\1#{delimiter}")
        formatted
      else
        num
      end
    end

    def on_windows?
      RUBY_PLATFORM =~ /mswin32|mingw32/
    end
  end
end
