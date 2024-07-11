#include <stdio.h>
#include <sys/stat.h>
#include <cephfs/ceph_ll_client.h>
#include <cephfs/libcephfs.h>
#include <filesystem>
#include <string>
#include <cstring>
#include <iostream>
#include <memory>
#include <cstdlib>

const std::string volume{"cephfs"};
const std::string sub_volume{"1"};
const std::string sub_volume_path{"volumes/_nogroup/1/"};
const std::string fs_path = sub_volume_path + "5763592a-833b-4250-9c31-5e6e23b5564e";

const std::string dir_name{"test-snapshot-dir"};
const std::string xattr_name{"user.test-snapshot-xattr"};
const std::string xattr_value{"test-snapshot-xattr-value"};
const std::string snap_name{"test-snapshot-snap"};

const std::filesystem::path config{"/etc/ceph/ceph.conf"};
const std::string client_id{"admin"};
const std::string client_uuid{"lx-2024-07-10"};

int Mount(std::shared_ptr<ceph_mount_info>& mount, std::shared_ptr<UserPerm>& user_perms) {
    namespace fs = std::filesystem;

  if (not fs::exists(config) or not fs::is_regular_file(config)) {
    std::cerr << "Unable to use " << config << " as a configuration file for ceph" << std::endl;
    return -EINVAL;
  }

  int result;

  {  // Create the mount point
    ceph_mount_info* cmount;
    result = ceph_create(&cmount, client_id.c_str());
    if (result) {
      std::cerr << "Failed to create ceph mount: error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
      return result;
    }

    mount = std::shared_ptr<ceph_mount_info>(
        cmount, [](ceph_mount_info* cmount) {
          if (cmount) {
            int result = ceph_unmount(cmount);
            if (result) {
              std::cerr << "Failed to unmount ceph mount: error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
            }

            result = ceph_release(cmount);
            if (result) {
                std::cerr << "Failed to release ceph mount: error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
            }
          }
        });
}

  // Read the configuration file
  result = ceph_conf_read_file(mount.get(), config.c_str());
  if (result) {
    std::cerr << "Failed read configuration file " << config << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  // Process any environment variables
  result = ceph_conf_parse_env(mount.get(), nullptr);
  if (result) {
    std::cerr << "Failed parse ceph environment variables: error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  result = ceph_conf_set(mount.get(), "debug_client", "1");
  if (result) {
    std::cerr << "Failed to set mount option debug_client value 1: error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  // Initialize the mount point
  result = ceph_init(mount.get());
  if (result) {
    std::cerr << "Failed to initialize ceph mount point: error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  std::cout << "Mounting ceph node" << std::endl;

  ceph_set_session_timeout(mount.get(), 60);

  result = ceph_start_reclaim(
      mount.get(), client_uuid.c_str(), CEPH_RECLAIM_RESET);
  if (result == -ENOTRECOVERABLE) {
    std::cerr << "Failed to start ceph reclaim" << std::endl;
    return result;
  } else if (result == -ENOENT) {
    std::cerr << "Not an error - Failed to start ceph reclaim" << std::endl;
  } else {
    std::cout << "Succeed on starting ceph reclaim" << std::endl;
  }

  ceph_finish_reclaim(mount.get());

  ceph_set_uuid(mount.get(), client_uuid.c_str());

  result = ceph_mount(mount.get(), nullptr);
  if (result) {
    std::cerr << "Failed to mount ceph: error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  {  // Get the user permissions
    UserPerm* perms = ceph_mount_perms(mount.get());
    if (not perms) {
      result = -EIO;
      std::cerr << "Failed get user perms: error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
      return result;
    }

    user_perms = std::shared_ptr<UserPerm>(perms, [](UserPerm*){});
  }

  return result;
}

typedef struct inodeno_t {
  uint64_t val;
} inodeno_t;

typedef struct snapid_t {
  uint64_t val;
} snapid_t;

typedef struct vinodeno_t {
  inodeno_t ino;
  snapid_t snapid;
} vinodeno_t;

int main(int, char**){
    std::shared_ptr<ceph_mount_info> mount;
    std::shared_ptr<UserPerm> user_perms;

    int result = Mount(mount, user_perms);
    if (result) {
        std::cerr << "Failed to mount ceph: error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    struct ceph_statx parent_sb;
    Inode* parent_inode = nullptr;

    result = ceph_ll_walk(mount.get(), fs_path.c_str(), &parent_inode, &parent_sb, CEPH_STATX_ALL_STATS, 0, user_perms.get());
    if (result) {
        std::cerr << "Failed to walk ceph path " << fs_path << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    std::shared_ptr<Inode> scoped_parent_inode(parent_inode, [mount](Inode* inode) {
        ceph_ll_put(mount.get(), inode);
    });

    struct ceph_statx dir_sb;
    Inode* test_dir_inode = nullptr;

    result = ceph_ll_mkdir(mount.get(), parent_inode, dir_name.c_str(), 0755, &test_dir_inode, &dir_sb, CEPH_STATX_ALL_STATS, 0, user_perms.get());
    if (result) {
        std::cerr << "Failed to create directory " << dir_name << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    std::shared_ptr<Inode> scoped_test_dir_inode(test_dir_inode, [mount](Inode* inode) {
        ceph_ll_put(mount.get(), inode);
    });

    result = ceph_ll_setxattr(mount.get(), test_dir_inode, xattr_name.c_str(), xattr_value.c_str(), xattr_value.size(), 0, user_perms.get());
    if (result) {
        std::cerr << "Failed to set xattr " << xattr_name << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    result = ceph_ll_getxattr(mount.get(), test_dir_inode, xattr_name.c_str(), nullptr, 0, user_perms.get());
    if (result < 0) {
        std::cerr << "Failed to get length of active dir's xattr " << xattr_name << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    std::string create_snap_cmd = "ceph fs subvolume snapshot create " + volume + " " + sub_volume + " " + snap_name;
   
    std::cout << create_snap_cmd << std::endl;

    result = system(create_snap_cmd.c_str());
    if (result) {
        std::cerr << "Failed to create snapshot " << snap_name << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    // Example: _test-snapshot-snap_1099511690785    
    struct ceph_statx sub_volume_sb;
    Inode* sub_volume_inode = nullptr;

    result = ceph_ll_walk(mount.get(), sub_volume_path.c_str(), 
                          &sub_volume_inode, &sub_volume_sb, CEPH_STATX_ALL_STATS, 0, user_perms.get());
    if (result) {
        std::cerr << "Failed to walk ceph path " << sub_volume_path << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    std::shared_ptr<Inode> scoped_sub_volume_inode(sub_volume_inode, [mount](Inode* inode) {
        ceph_ll_put(mount.get(), inode);
    });

    std::string snap_path = fs_path + "/.snap/_" + snap_name + "_" + std::to_string(sub_volume_sb.stx_ino);
    struct ceph_statx snap_sb;

    result = ceph_statx(mount.get(), snap_path.c_str(), &snap_sb, CEPH_STATX_ALL_STATS, 0);
    if (result) {
        std::cerr << "Failed to statx snapshot path " << snap_path << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    const uint64_t snap_id = snap_sb.stx_dev;
    Inode* test_dir_inode_snap = nullptr;
    const vinodeno vivo = {dir_sb.stx_ino, snap_id};

    result = ceph_ll_lookup_vino(mount.get(), vivo, &test_dir_inode_snap);
    if (result) {
        std::cerr << "Failed to lookup inode of directory {" << std::to_string(vivo.ino.val) << ", " << std::to_string(vivo.snapid.val) << "} in snapshot: error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    std::shared_ptr<Inode> scoped_test_dir_inode_snap(test_dir_inode_snap, [mount](Inode* inode) {
        ceph_ll_put(mount.get(), inode);
    });

    //const char* new_xattr_name = "ceph.snap.btime";
    const char* new_xattr_name = xattr_name.c_str();

    result = ceph_ll_getxattr(mount.get(), test_dir_inode_snap, new_xattr_name, nullptr, 0, user_perms.get());
    if (result < 0) {
        std::cerr << "Failed to get length of snapshot file's xattr " << new_xattr_name << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    std::string xattr_read(result + 1, '\0');

    result = ceph_ll_getxattr(mount.get(), test_dir_inode_snap, new_xattr_name, xattr_read.data(), result, user_perms.get());
    if (result < 0) {
        std::cerr << "Failed to get snapshot file's xattr " << new_xattr_name << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    std::cerr << "xattr of dir in snapshot: " << snap_id <<  " is: "<< xattr_read << std::endl;

    return 0;
}
