namespace 'pkg' do
  desc "build Mac OS X pkg package"
  task 'build' => '^build' do
    create_build_dir('pkg') do |dir|
      FileUtils.mkdir_p "bundle"
      FileUtils.mkdir_p "bundle/Resources"
      FileUtils.mkdir_p "bundle/td-client.pkg"
      FileUtils.mkdir_p "bundle/td-client.pkg"

      # create ./bundle/td-client.pkg/Payload
      mkchdir('td-client.build') do
        mkchdir('vendor/gems') do
          install_use_gems(Dir.pwd)
        end
        install_resource 'pkg/td', 'bin/td', 0755
        sh "pax -wz -x cpio . > ../bundle/td-client.pkg/Payload"
      end

      zip_files(project_root_path("pkg/td-update-pkg-#{version}.zip"), 'td-client.build')

      # create ./bundle/td-client.pkg/Bom
      sh "mkbom -s td-client.build bundle/td-client.pkg/Bom"

      # create ./bundle/td-client.pkg/Scripts/
      install_resource 'pkg/postinstall', 'bundle/td-client.pkg/Scripts/postinstall', 0755

      variables = {
        :version => version,
        :kbytes => `du -ks td-client.build | cut -f 1`.strip.to_i,
        :num_files => `find td-client.build | wc -l`,
      }

      # create ./bundle/td-client.pkg/PackageInfo
      install_erb_resource('pkg/PackageInfo.erb', 'bundle/td-client.pkg/PackageInfo', 0644, variables)

      # create ./bundle/Distribution
      install_erb_resource('pkg/Distribution.erb', 'bundle/Distribution', 0644, variables)

      sh "pkgutil --expand #{project_root_path('dist/resources/pkg/ruby-2.0.0-p0.pkg')} ruby"
      mv "ruby/ruby-2.0.0-p0.pkg", "bundle/ruby.pkg"

      # create td-a.b.c.pkg
      sh "pkgutil --flatten bundle td-#{version}.pkg"
      FileUtils.cp "td-#{version}.pkg", project_root_path("pkg/td-#{version}.pkg")
    end
  end

  desc "clean Mac OS X pkg package"
  task "clean" do
    FileUtils.rm_rf build_dir_path('pkg')
    FileUtils.rm_rf project_root_path("pkg/td-#{version}.pkg")
    FileUtils.rm_rf project_root_path("pkg/td-update-pkg-#{version}.zip")
  end
end
