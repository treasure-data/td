module TreasureData
  module Helpers
    module_function

    def on_windows?
      RUBY_PLATFORM =~ /mswin32|mingw32/
    end
  end
end
