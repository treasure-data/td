require "erb"

file pkg("td-#{version}.exe") do |t|
  tempdir do |dir|
    mkchdir("installers") do
      # TODO http://rubyinstaller.org/
      system "curl http://heroku-toolbelt.s3.amazonaws.com/rubyinstaller.exe -o rubyinstaller.exe"
    end

    mkchdir("td") do
      assemble_distribution
      assemble_gems
      assemble resource("exe/td"), "bin/td", 0755
      assemble resource("exe/td.bat"), "bin/td.bat", 0755
    end

    File.open("td.iss", "w") do |iss|
      iss.write(ERB.new(File.read(resource("exe/td.iss"))).result(binding))
    end

    inno_dir = ENV["INNO_DIR"] || 'C:\\Program Files (x86)\\Inno Setup 5\\'

    system "\"#{inno_dir}\\Compil32.exe\" /cc \"td.iss\""

    raise "Inno Setup failed with code=#{$?}" if $?.to_i != 0
  end
end

desc 'build exe'
task 'exe:build' => pkg("td-#{version}.exe")

desc 'clean exe'
task 'exe:clean' => pkg("td-#{version}.exe")
