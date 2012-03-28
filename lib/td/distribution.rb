module TreasureData
  module Distribution
    def self.files
      fs = Dir[File.expand_path("../../../{bin,data,lib}/**/*", __FILE__)].select do |file|
        File.file?(file)
      end
      fs << File.expand_path("../../../Gemfile", __FILE__)
      fs << File.expand_path("../../../td.gemspec", __FILE__)
      fs
    end
  end
end
