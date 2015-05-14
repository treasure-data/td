module TreasureData
  module Helpers
    module_function

    def format_with_delimiter(number, delimiter = ',')
      number.to_s.gsub(/(\d)(?=(?:\d{3})+(?!\d))/, "\\1#{delimiter}")
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
