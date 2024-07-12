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
#include <functional>
#include <dirent.h>
#include <queue>

const std::string volume{"cephfs"};
const std::string sub_volume{"1"};
const std::string sub_volume_path{"volumes/_nogroup/1/"};
const std::string fs_path = sub_volume_path + "5763592a-833b-4250-9c31-5e6e23b5564e";
const std::string snap_dir = fs_path + "/.snap";

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

using DirEntryCallback =
      std::function<bool(const std::string& name, const struct ceph_statx& sb, std::shared_ptr<Inode>)>;

int ReadDir(std::shared_ptr<ceph_mount_info> mount, std::shared_ptr<Inode> parent, DirEntryCallback callback) {

  struct ceph_dir_result* dh_parent = nullptr;

  int result = ceph_ll_opendir(mount.get(), parent.get(), &dh_parent, ceph_mount_perms(mount.get()));
  if (result) {
      std::cerr << "Failed to open directory: error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
      return result;
  }

  std::shared_ptr<ceph_dir_result> scoped_dh_parent(dh_parent, [mount](ceph_dir_result* dh) {
      ceph_ll_releasedir(mount.get(), dh);
  });

  bool done = false;

  do {
    dirent entry;
    struct ceph_statx sb;
    struct Inode* ceph_inode;

    result = ceph_readdirplus_r(mount.get(), dh_parent, &entry, &sb,
                                         CEPH_STATX_ALL_STATS, 0, &ceph_inode);
    if (result < 0) {
      std::cerr << "Failed to read directory: error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
      break;
    }

    if (result == 0) {
      break;
    }

    auto eh = std::shared_ptr<Inode>(ceph_inode, [mount](struct Inode* inode) {
            ceph_ll_put(mount.get(), inode);
          });

    const std::string entry_name(entry.d_name);

    done = not callback(entry_name, sb, eh);
  } while (not done);

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

    struct ceph_statx sb_fs;
    Inode* inode_fs = nullptr;

    result = ceph_ll_walk(mount.get(), fs_path.c_str(), &inode_fs, &sb_fs, CEPH_STATX_ALL_STATS, 0, user_perms.get());
    if (result) {
        std::cerr << "Failed to walk ceph path " << fs_path << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    std::shared_ptr<Inode> scoped_parent_inode(inode_fs, [mount](Inode* inode) {
        ceph_ll_put(mount.get(), inode);
    });

    struct ceph_statx dir_sb;
    Inode* test_dir_inode = nullptr;

    result = ceph_ll_mkdir(mount.get(), inode_fs, dir_name.c_str(), 0755, &test_dir_inode, &dir_sb, CEPH_STATX_ALL_STATS, 0, user_perms.get());
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

  #if 1
    std::string create_snap_cmd = "ceph fs subvolume snapshot create " + volume + " " + sub_volume + " " + snap_name;
    std::cout << create_snap_cmd << std::endl;
    result = system(create_snap_cmd.c_str());
    if (result) {
        std::cerr << "Failed to create snapshot " << snap_name << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

  
  #else 
    result = ceph_mksnap(mount.get(), fs_path.c_str(), snap_name.c_str(), 0755, nullptr, 0);
    if (result) {
        std::cerr << "Failed to create snapshot " << snap_name << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    std::string snap_path = snap_dir + "/" + snap_name;
  #endif 

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

    const std::string snap_dir_name = "_" + snap_name + "_" + std::to_string(sub_volume_sb.stx_ino);
    std::string snap_path = snap_dir + "/" + snap_dir_name;
    struct ceph_statx snap_sb;

    result = ceph_statx(mount.get(), snap_path.c_str(), &snap_sb, CEPH_STATX_ALL_STATS, 0);
    if (result) {
        std::cerr << "Failed to statx snapshot path " << snap_path << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    snap_info snap_info;

    result = ceph_get_snap_info(mount.get(), snap_path.c_str(), &snap_info);
    if (result) {
        std::cerr << "Failed to get snap info of snapshot path " << snap_path << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    const uint64_t snap_id = snap_info.id;
    const vinodeno vivo = {dir_sb.stx_ino, snap_id};

    Inode* test_dir_inode_snap = nullptr;
    result = ceph_ll_lookup_vino(mount.get(), vivo, &test_dir_inode_snap);
    if (result) {
        std::cerr << "Failed to lookup inode of directory {" << std::to_string(vivo.ino.val) << ", " << std::to_string(vivo.snapid.val) << "} in snapshot: error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    std::shared_ptr<Inode> scoped_test_dir_inode_snap(test_dir_inode_snap, [mount](Inode* inode) {
        ceph_ll_put(mount.get(), inode);
    });


    struct ceph_statx dir_sb_snap_try;
    result = ceph_ll_getattr(mount.get(), test_dir_inode_snap, &dir_sb_snap_try, CEPH_STATX_ALL_STATS, 0, user_perms.get());
    if (result < 0) {
        std::cerr << "Failed to stat snapshot file" << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

#if 0
    const std::string dir_snap_path = snap_path + "/" + dir_name;
    struct ceph_statx dir_sb_snap;

    result = ceph_statx(mount.get(), dir_snap_path.c_str(), &dir_sb_snap, CEPH_STATX_ALL_STATS, 0);
    if (result) {
        std::cerr << "Failed to statx directory in snapshot " << dir_snap_path << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }
#elif 0
    struct ceph_statx sb_snap_dir;
    result = ceph_statx(mount.get(), snap_dir.c_str(), &sb_snap_dir, CEPH_STATX_ALL_STATS, 0);
    if (result) {
        std::cerr << "Failed to statx snapshot dir " << snap_dir << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    Inode* inode_snap_dir = nullptr;
    result = ceph_ll_lookup_vino(mount.get(), {sb_snap_dir.stx_ino, sb_snap_dir.stx_dev}, &inode_snap_dir);
    if (result) {
        std::cerr << "Failed to lookup inode of .snap directory error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }
    
    Inode* inode_snap_path = nullptr;
    struct ceph_statx sb_snap_path;
    result = ceph_ll_lookup(mount.get(), inode_snap_dir, snap_dir_name.c_str(), &inode_snap_path, &sb_snap_path, CEPH_STATX_INO, 0, user_perms.get());
    if (result) {
        std::cerr << "Failed to lookup inode of snapshot path " << snap_dir_name << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    std::shared_ptr<Inode> scoped_inode_snap_path(inode_snap_path, [mount](Inode* inode) {
        ceph_ll_put(mount.get(), inode);
    });

    Inode* inode_snap_sub_dir = nullptr;
    struct ceph_statx sb_snap_sub_dir;

    result = ceph_ll_lookup(mount.get(), inode_snap_path, dir_name.c_str(), &inode_snap_sub_dir, &sb_snap_sub_dir, CEPH_STATX_INO, 0, user_perms.get());
    if (result) {
        std::cerr << "Failed to lookup inode of snapshot sub directory " << dir_name << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    std::shared_ptr<Inode> scoped_inode_snap_sub_dir(inode_snap_sub_dir, [mount](Inode* inode) {
        ceph_ll_put(mount.get(), inode);
    });
#else
  Inode* inode_snap_dir = nullptr;
  struct ceph_statx sb_snap_dir;

  result = ceph_ll_lookup(mount.get(), inode_fs, ".snap", &inode_snap_dir, &sb_snap_dir, CEPH_STATX_INO, 0, user_perms.get());
  if (result) {
      std::cerr << "Failed to lookup inode of .snap directory: error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
      return result;
  }

  auto scoped_inode_snap_dir = std::shared_ptr<Inode>(inode_snap_dir, [mount](Inode* inode) {
      ceph_ll_put(mount.get(), inode);
  });
    
  std::shared_ptr<Inode> scoped_inode_parent;

  auto ecb = [snap_id, &scoped_inode_parent](const std::string& name, const struct ceph_statx& sb, std::shared_ptr<Inode> inode){
    if (sb.stx_dev == snap_id) {
      scoped_inode_parent = inode;
      return false;
    }
    return true;
  };

  result = ReadDir(mount, scoped_inode_snap_dir, ecb);
  if (not scoped_inode_parent) {
    std::cerr << "Failed to find snapshot inode" << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  std::shared_ptr<Inode> scoped_inode_my;
  std::deque<std::shared_ptr<Inode>> inode_queue;

  while (true) {  
    bool done = false;

    auto ecb = [&dir_sb, &scoped_inode_my, &inode_queue](const std::string& name, const struct ceph_statx& sb, std::shared_ptr<Inode> inode){
      bool next = true;

      if (sb.stx_ino == dir_sb.stx_ino) {
        scoped_inode_my = inode;
        next = false;
      } else if (S_ISDIR(sb.stx_mode) and sb.stx_nlink > 0) {
        inode_queue.push_back(inode);
      } 
      
      return next;
    };

    result = ReadDir(mount, scoped_inode_parent, ecb);

    if (scoped_inode_my) {
      break;
    }

    if (inode_queue.empty()) {
      break;
    }

    scoped_inode_parent = inode_queue.front();
    inode_queue.pop_front(); 
  }

  if (not scoped_inode_my) {
    std::cerr << "Not found snapshot inode" << std::endl;
    return -EINVAL;
  }

  if (scoped_inode_my.get() != test_dir_inode_snap) {
    std::cerr << "Mismatched snapshot file handles" << std::endl;
    return -EINVAL;
  }
    
#endif 

    struct ceph_statx dir_sb_snap;
    result = ceph_ll_getattr(mount.get(), test_dir_inode_snap, &dir_sb_snap, CEPH_STATX_ALL_STATS, 0, user_perms.get());
    if (result < 0) {
        std::cerr << "Failed to stat snapshot file" << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
        return result;
    }

    std::cout << "snapshot inode " << dir_sb_snap.stx_dev << " >=< " << dir_sb.stx_dev << std::endl;

    if (dir_sb_snap.stx_dev != dir_sb_snap_try.stx_dev) {
        std::cerr << "Snapshot inode device mismatch: " << dir_sb_snap.stx_dev << " != " << dir_sb_snap_try.stx_dev << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_ino != dir_sb_snap_try.stx_ino) {
        std::cerr << "Snapshot inode number mismatch: " << dir_sb_snap.stx_ino << " != " << dir_sb_snap_try.stx_ino << std::endl;
        return -EINVAL;
    }
    
    if (dir_sb_snap.stx_size != dir_sb_snap_try.stx_size) {
        std::cerr << "Snapshot inode size mismatch: " << dir_sb_snap.stx_size << " != " << dir_sb_snap_try.stx_size << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_blocks != dir_sb_snap_try.stx_blocks) {
        std::cerr << "Snapshot inode blocks mismatch: " << dir_sb_snap.stx_blocks << " != " << dir_sb_snap_try.stx_blocks << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_mode != dir_sb_snap_try.stx_mode) {
        std::cerr << "Snapshot inode mode mismatch: " << dir_sb_snap.stx_mode << " != " << dir_sb_snap_try.stx_mode << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_uid != dir_sb_snap_try.stx_uid) {
        std::cerr << "Snapshot inode uid mismatch: " << dir_sb_snap.stx_uid << " != " << dir_sb_snap_try.stx_uid << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_gid != dir_sb_snap_try.stx_gid) {
        std::cerr << "Snapshot inode gid mismatch: " << dir_sb_snap.stx_gid << " != " << dir_sb_snap_try.stx_gid << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_nlink != dir_sb_snap_try.stx_nlink) {
        std::cerr << "Snapshot inode nlink mismatch: " << dir_sb_snap.stx_nlink << " != " << dir_sb_snap_try.stx_nlink << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_rdev != dir_sb_snap_try.stx_rdev) {
        std::cerr << "Snapshot inode rdev mismatch: " << dir_sb_snap.stx_rdev << " != " << dir_sb_snap_try.stx_rdev << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_blksize != dir_sb_snap_try.stx_blksize) {
        std::cerr << "Snapshot inode blksize mismatch: " << dir_sb_snap.stx_blksize << " != " << dir_sb_snap_try.stx_blksize << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_version != dir_sb_snap_try.stx_version) {
        std::cerr << "Snapshot inode version mismatch: " << dir_sb_snap.stx_version << " != " << dir_sb_snap_try.stx_version << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_atime.tv_sec != dir_sb_snap_try.stx_atime.tv_sec) {
        std::cerr << "Snapshot inode atime sec mismatch: " << dir_sb_snap.stx_atime.tv_sec << " != " << dir_sb_snap_try.stx_atime.tv_sec << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_atime.tv_nsec != dir_sb_snap_try.stx_atime.tv_nsec) {
        std::cerr << "Snapshot inode atime nsec mismatch: " << dir_sb_snap.stx_atime.tv_nsec << " != " << dir_sb_snap_try.stx_atime.tv_nsec << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_ctime.tv_sec != dir_sb_snap_try.stx_ctime.tv_sec) {
        std::cerr << "Snapshot inode ctime sec mismatch: " << dir_sb_snap.stx_ctime.tv_sec << " != " << dir_sb_snap_try.stx_ctime.tv_sec << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_ctime.tv_nsec != dir_sb_snap_try.stx_ctime.tv_nsec) {
        std::cerr << "Snapshot inode ctime nsec mismatch: " << dir_sb_snap.stx_ctime.tv_nsec << " != " << dir_sb_snap_try.stx_ctime.tv_nsec << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_mtime.tv_sec != dir_sb_snap_try.stx_mtime.tv_sec) {
        std::cerr << "Snapshot inode mtime sec mismatch: " << dir_sb_snap.stx_mtime.tv_sec << " != " << dir_sb_snap_try.stx_mtime.tv_sec << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_mtime.tv_nsec != dir_sb_snap_try.stx_mtime.tv_nsec) {
        std::cerr << "Snapshot inode mtime nsec mismatch: " << dir_sb_snap.stx_mtime.tv_nsec << " != " << dir_sb_snap_try.stx_mtime.tv_nsec << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_btime.tv_sec != dir_sb_snap_try.stx_btime.tv_sec) {
        std::cerr << "Snapshot inode btime sec mismatch: " << dir_sb_snap.stx_btime.tv_sec << " != " << dir_sb_snap_try.stx_btime.tv_sec << std::endl;
        return -EINVAL;
    }

    if (dir_sb_snap.stx_btime.tv_nsec != dir_sb_snap_try.stx_btime.tv_nsec) {
        std::cerr << "Snapshot inode btime nsec mismatch: " << dir_sb_snap.stx_btime.tv_nsec << " != " << dir_sb_snap_try.stx_btime.tv_nsec << std::endl;
        return -EINVAL;
    }

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
