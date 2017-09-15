namespace 'exe' do
  desc "build Windows exe package"
  task 'build' => '^build' do
    create_build_dir('exe') do |dir|
      install_ruby_version = '2.2.8'
      # create ./installers/
      FileUtils.mkdir_p "installers"
      installer_path = project_root_path("dist/resources/exe/rubyinstaller-2.2.8.exe")
      FileUtils.cp installer_path, "installers/rubyinstaller.exe"

      variables = {
        :version => version,
        :basename => "td-#{version}",
        :outdir => ".",
        :install_ruby_version => install_ruby_version,
      }

      # create ./td/
      mkchdir("td") do
        mkchdir('vendor/gems') do
          install_use_gems(Dir.pwd)
        end
        install_resource 'exe/td', 'bin/td', 0755
        install_erb_resource 'exe/td.bat', 'bin/td.bat', 0755, variables
        install_resource 'exe/td-cmd.bat', 'td-cmd.bat', 0755
      end

      zip_files(project_root_path("pkg/td-update-exe-#{version}.zip"), 'td')

      # create td.iss and run Inno Setup
      install_erb_resource 'exe/td.iss', 'td.iss', 0644, variables

      inno_dir = ENV["INNO_DIR"] || 'C:/Program Files (x86)/Inno Setup 5'
      inno_bin = ENV["INNO_BIN"] || "#{inno_dir}/Compil32.exe"
      puts "INNO_BIN: #{inno_bin}"

      sh "\"#{inno_bin}\" /cc \"td.iss\""
      FileUtils.cp "td-#{version}.exe", project_root_path("pkg/td-#{version}.exe")
    end
  end

  desc "clean Windows exe package"
  task "clean" do
    FileUtils.rm_rf build_dir_path('exe')
    FileUtils.rm_rf project_root_path("pkg/td-#{version}.exe")
    FileUtils.rm_rf project_root_path("pkg/td-update-exe-#{version}.zip")
  end
end
