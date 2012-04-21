require "erb"

file pkg("td-#{version}.exe") do |t|
  tempdir do |dir|
    mkchdir("td-client") do
      assemble_distribution
      assemble_gems
      assemble resource("pkg/td"), "bin/td", 0755
    end

    mkchdir("installers") do
      # TODO http://rubyinstaller.org/
      system "curl http://heroku-toolbelt.s3.amazonaws.com/rubyinstaller.exe -o rubyinstaller.exe"
    end

    cp resource("exe/td.bat"), "td-client/bin/td.bat"
    cp resource("exe/td"),     "td-client/bin/td"

    File.open("td.iss", "w") do |iss|
      iss.write(ERB.new(File.read(resource("exe/td.iss"))).result(binding))
    end

    inno_dir = ENV["INNO_DIR"] || 'C:\\Program Files (x86)\\Inno Setup 5\\'

    system "\"#{inno_dir}\\Compil32.exe\" /cc \"td.iss\""
  end
end

desc 'build exe'
task 'exe:build' => pkg("td-#{version}.exe")

desc 'clean exe'
task 'exe:clean' => pkg("td-#{version}.exe")
