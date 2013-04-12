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

    def home_directory
      on_windows? ? ENV['USERPROFILE'].gsub("\\","/") : ENV['HOME']
    end

    def on_windows?
      RUBY_PLATFORM =~ /mswin32|mingw32/
    end

    def on_mac?
      RUBY_PLATFORM =~ /-darwin\d/
    end
  end
end
