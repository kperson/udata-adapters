Vagrant.configure(2) do |config|

  config.vm.box = "tallypix/tallyho"
  config.vm.box_url = "https://s3.amazonaws.com/tallyhovirtualbox/tallyho.json"
  config.vm.box_version = "0.3.0"
  config.ssh.password = "vagrant"
  config.ssh.username = "vagrant"

  config.vm.provider :virtualbox do |p|
    p.name = "udata-adpaters"
    p.memory = 4096
    p.cpus = 4
    p.customize ["modifyvm", :id, "--cpuexecutioncap", "85"]
  end

  config.vm.hostname = "adapters.udata.net"
  config.vm.provision "shell", inline: "wget -O sbt.deb https://bintray.com/artifact/download/sbt/debian/sbt-0.13.9.deb && dpkg -i sbt.deb"
  config.vm.synced_folder ".", "/vagrant", type: "nfs", nfs_udp: false, :mount_options => ['nolock,noatime']
  config.vm.network "private_network", ip: "172.16.100.101"
  config.vm.network :forwarded_port, guest: 22, host: 2204
end
