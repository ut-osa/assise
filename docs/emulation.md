# Setting up NVDIMM Emulation for Assise

### Check kernel supports emulation

Emulation is supported by the kernel versions 4.0+ but it is recommended to use versions higher then 4.2 due to ease of configuration.

### Getting the kernel source
 
Get the current kernel version using uname version.

    uname -r
    5.6.0-2-amd64

Download the kernel source files. For systems with apt-get, you can do so as follows:
 
    sudo apt-get install linux-source-5.6
    cp /usr/src/linux-sources-5.6.tar.xz
    tar xf linux-sources-5.6.tar.xz
    cd linux-sources-5.6

### Enabling features
 
    make nconfig

Mark these features as enabled by pressing y. Save the configuration by pressing the escape key.

    Processor type and features --->
		[*] Support non-standard NVDIMMs and ADR protected memory
	File systems --->
		[*] Direct Access (DAX) support

Also change the .config file and set:
 
    CONFIG_SYSTEM_TRUSTED_KEYS = ""


### Installing new kernel
Then make the kernel and install it. The kernel build will be in the directory above the sources directory.
 
    make deb-pkg
    sudo dpkg -i linux-image.x.y.z.deb 
    sudo shutdown -r now

### Setting up kernel parameters

To find the proper kernel parameters, you need to find the set of available memory regions. You can do so by running the following command:
    dmesg | grep BIOS-e820

    [    0.000000] BIOS-e820: [mem 0x00000000feffc000-0x00000000feffffff] reserved
    [    0.000000] BIOS-e820: [mem 0x00000000fffc0000-0x00000000ffffffff] reserved
    [    0.000000] BIOS-e820: [mem 0x0000000100000000-0x000000053fffffff] usable

Pick a region of memory that is marked as usable. e.g.

    [    0.000000] BIOS-e820: [mem 0x0000000100000000-0x000000053fffffff] usable


The region above starts at 0x0000000100000000 (4 GB) and ends at 0x000000053fffffff (21 GB). We can configure our machine to create a pmem memory region by passing the following kernel parameters:

    memmap=8G!4G

This will reserve 8 GB of memory starting at the 4 GB offset (which is usable in this example). On an Ubuntu 18.04 BIOS machine, you can pass these parameters by modifying the grub configuration file.

    vi /etc/default/grub
    GRUB_CMDLINE_LINUX="memmap=8G!4G"

After editing this file, run the following command to apply your changes:

    update-grub2 && update-grub

After rebooting, you should see one pmem region configured.
 
    ndctl list

    [
    {
      "dev":"namespace1.0",
    "mode":"fsdax",
    "map":"mem",
    "size":8589934592,
    "sector_size":512,
    "blockdev":"pmem0"
    } 
    ]

### More info
[Choosing pmem kernel parameters](https://nvdimm.wiki.kernel.org/how_to_choose_the_correct_memmap_kernel_parameter_for_pmem_on_your_system)
