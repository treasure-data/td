require 'erb'

file pkg("td-#{version}.pkg") => distribution_files("pkg") do |t|
  tempdir do |dir|
    mkchdir("td-client") do
      assemble_distribution
      assemble_gems
      assemble resource("pkg/td"), "bin/td", 0755
    end

    kbytes = %x{ du -ks td-client | cut -f 1 }
    num_files = %x{ find td-client | wc -l }

    mkdir_p "pkg"
    mkdir_p "pkg/Resources"
    mkdir_p "pkg/td-client.pkg"

    dist = File.read(resource("pkg/Distribution.erb"))
    dist = ERB.new(dist).result(binding)
    File.open("pkg/Distribution", "w") { |f| f.puts dist }

    dist = File.read(resource("pkg/PackageInfo.erb"))
    dist = ERB.new(dist).result(binding)
    File.open("pkg/td-client.pkg/PackageInfo", "w") { |f| f.puts dist }

    mkdir_p "pkg/td-client.pkg/Scripts"
    cp resource("pkg/postinstall"), "pkg/td-client.pkg/Scripts/postinstall"
    chmod 0755, "pkg/td-client.pkg/Scripts/postinstall"

    sh %{ mkbom -s td-client pkg/td-client.pkg/Bom }

    Dir.chdir("td-client") do
      sh %{ pax -wz -x cpio . > ../pkg/td-client.pkg/Payload }
    end

    sh %{ pkgutil --flatten pkg td-#{version}.pkg }

    cp_r "td-#{version}.pkg", t.name
  end
end

desc 'build pkg'
task 'pkg:build' => pkg("td-#{version}.pkg")

desc 'clean pkg'
task 'pkg:clean' => pkg("td-#{version}.pkg")
