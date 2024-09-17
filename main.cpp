#include <cephfs/libcephfs.h>
#include <dirent.h>
#include <stdio.h>
#include <sys/stat.h>

#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <queue>
#include <string>

const std::string volume{"cephfs"};
const std::string sub_volume{"1"};
const std::string sub_volume_path{"volumes/_nogroup/1/"};
const std::string fs_path =
    sub_volume_path + "7fdca653-41a3-4c47-bf85-1848d3f104d2";
const std::string snap_dir = fs_path + "/.snap";

const std::string dir_name{"test-snapshot-dir"};
const std::string sub_dir_name{"test-snapshot-sub-dir"};
const std::string file_name{"test-snapshot-file"};
const std::string xattr_name{"user.test-snapshot-xattr"};
const std::string xattr_value{"test-snapshot-xattr-value"};
const std::string snap_name{"test-snapshot"};

const std::filesystem::path config{"/etc/ceph/ceph.conf"};
const std::string client_id{"admin"};
const std::string client_uuid{"lx-2024-07-10"};

int Mount(std::shared_ptr<ceph_mount_info>& mount,
          std::shared_ptr<UserPerm>& user_perms) {
  namespace fs = std::filesystem;

  if (not fs::exists(config) or not fs::is_regular_file(config)) {
    std::cerr << "Unable to use " << config
              << " as a configuration file for ceph" << std::endl;
    return -EINVAL;
  }

  int result;

  {  // Create the mount point
    ceph_mount_info* cmount;
    result = ceph_create(&cmount, client_id.c_str());
    if (result) {
      std::cerr << "Failed to create ceph mount: error " << -result << " ("
                << ::strerror(-result) << ")" << std::endl;
      return result;
    }

    mount =
        std::shared_ptr<ceph_mount_info>(cmount, [](ceph_mount_info* cmount) {
          if (cmount) {
            int result = ceph_unmount(cmount);
            if (result) {
              std::cerr << "Failed to unmount ceph mount: error " << -result
                        << " (" << ::strerror(-result) << ")" << std::endl;
            }

            result = ceph_release(cmount);
            if (result) {
              std::cerr << "Failed to release ceph mount: error " << -result
                        << " (" << ::strerror(-result) << ")" << std::endl;
            }
          }
        });
  }

  // Read the configuration file
  result = ceph_conf_read_file(mount.get(), config.c_str());
  if (result) {
    std::cerr << "Failed read configuration file " << config << ": error "
              << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  // Process any environment variables
  result = ceph_conf_parse_env(mount.get(), nullptr);
  if (result) {
    std::cerr << "Failed parse ceph environment variables: error " << -result
              << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  result = ceph_conf_set(mount.get(), "debug_client", "1");
  if (result) {
    std::cerr << "Failed to set mount option debug_client value 1: error "
              << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  // Initialize the mount point
  result = ceph_init(mount.get());
  if (result) {
    std::cerr << "Failed to initialize ceph mount point: error " << -result
              << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  std::cout << "Mounting ceph node" << std::endl;

  ceph_set_session_timeout(mount.get(), 60);

  result =
      ceph_start_reclaim(mount.get(), client_uuid.c_str(), CEPH_RECLAIM_RESET);
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
    std::cerr << "Failed to mount ceph: error " << -result << " ("
              << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  {  // Get the user permissions
    UserPerm* perms = ceph_mount_perms(mount.get());
    if (not perms) {
      result = -EIO;
      std::cerr << "Failed get user perms: error " << -result << " ("
                << ::strerror(-result) << ")" << std::endl;
      return result;
    }

    user_perms = std::shared_ptr<UserPerm>(perms, [](UserPerm*) {});
  }

  return result;
}

using DirEntryCallback =
    std::function<bool(const std::string& name, const struct ceph_statx& sb,
                       std::shared_ptr<Inode>)>;

int ReadDir(std::shared_ptr<ceph_mount_info> mount,
            std::shared_ptr<Inode> parent, DirEntryCallback callback) {
  struct ceph_dir_result* dh_parent = nullptr;

  int result = ceph_ll_opendir(mount.get(), parent.get(), &dh_parent,
                               ceph_mount_perms(mount.get()));
  if (result) {
    std::cerr << "Failed to open directory: error " << -result << " ("
              << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  std::shared_ptr<ceph_dir_result> scoped_dh_parent(
      dh_parent,
      [mount](ceph_dir_result* dh) { ceph_ll_releasedir(mount.get(), dh); });

  bool done = false;

  do {
    dirent entry;
    struct ceph_statx sb;
    struct Inode* ceph_inode;

    result = ceph_readdirplus_r(mount.get(), dh_parent, &entry, &sb,
                                CEPH_STATX_ALL_STATS, 0, &ceph_inode);
    if (result < 0) {
      std::cerr << "Failed to read directory: error " << -result << " ("
                << ::strerror(-result) << ")" << std::endl;
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

int prepare(std::shared_ptr<ceph_mount_info> mount, struct ceph_statx& dir_sb, 
            struct ceph_statx& sub_dir_sb, struct ceph_statx& file_sb) {
  struct ceph_statx sb_fs;
  Inode* inode_fs = nullptr;

  int result =
      ceph_ll_walk(mount.get(), fs_path.c_str(), &inode_fs, &sb_fs,
                   CEPH_STATX_ALL_STATS, 0, ceph_mount_perms(mount.get()));
  if (result) {
    std::cerr << "Failed to walk ceph path " << fs_path << ": error " << -result
              << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  std::shared_ptr<Inode> scoped_parent_inode(
      inode_fs, [mount](Inode* inode) { ceph_ll_put(mount.get(), inode); });

  Inode* test_dir_inode = nullptr;

  result = ceph_ll_mkdir(mount.get(), inode_fs, dir_name.c_str(), 0755,
                         &test_dir_inode, &dir_sb, CEPH_STATX_ALL_STATS, 0,
                         ceph_mount_perms(mount.get()));
  if (result) {
    std::cerr << "Failed to create directory " << dir_name << ": error "
              << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  std::shared_ptr<Inode> scoped_test_dir_inode(
      test_dir_inode,
      [mount](Inode* inode) { ceph_ll_put(mount.get(), inode); });

  result = ceph_ll_setxattr(mount.get(), test_dir_inode, xattr_name.c_str(),
                            xattr_value.c_str(), xattr_value.size(), 0,
                            ceph_mount_perms(mount.get()));
  if (result) {
    std::cerr << "Failed to set xattr " << xattr_name << ": error " << -result
              << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  result = ceph_ll_getxattr(mount.get(), test_dir_inode, xattr_name.c_str(),
                            nullptr, 0, ceph_mount_perms(mount.get()));
  if (result < 0) {
    std::cerr << "Failed to get length of active dir's xattr " << xattr_name
              << ": error " << -result << " (" << ::strerror(-result) << ")"
              << std::endl;
    return result;
  }

  Inode* test_sub_dir_inode = nullptr;

  result = ceph_ll_mkdir(mount.get(), test_dir_inode, sub_dir_name.c_str(), 0755,
                         &test_sub_dir_inode, &sub_dir_sb, CEPH_STATX_ALL_STATS, 0,
                         ceph_mount_perms(mount.get()));
  if (result) {
    std::cerr << "Failed to create sub directory " << sub_dir_name << ": error "
              << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  std::shared_ptr<Inode> scoped_test_sub_dir_inode(
      test_sub_dir_inode,
      [mount](Inode* inode) { ceph_ll_put(mount.get(), inode); });

  result = ceph_ll_setxattr(mount.get(), test_sub_dir_inode, xattr_name.c_str(),
                            xattr_value.c_str(), xattr_value.size(), 0,
                            ceph_mount_perms(mount.get()));
  if (result) {
    std::cerr << "Failed to set sub-dir's xattr " << xattr_name << ": error " << -result
              << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  result = ceph_ll_getxattr(mount.get(), test_sub_dir_inode, xattr_name.c_str(),
                            nullptr, 0, ceph_mount_perms(mount.get()));
  if (result < 0) {
    std::cerr << "Failed to get length of active sub-dir's xattr " << xattr_name
              << ": error " << -result << " (" << ::strerror(-result) << ")"
              << std::endl;
    return result;
  }

  Fh* fhp_test_file = nullptr;
  Inode* test_file_inode = nullptr;

  result = ceph_ll_create(mount.get(), inode_fs, file_name.c_str(), 0755,
                          O_CREAT | O_WRONLY, &test_file_inode, &fhp_test_file,
                          &file_sb, CEPH_STATX_ALL_STATS, 0,
                          ceph_mount_perms(mount.get()));
  if (result) {
    std::cerr << "Failed to create file " << file_name << ": error " << -result
              << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  result = ceph_ll_write(mount.get(), fhp_test_file, 0, 9, "some data");
  if (result < 0) {
    std::cerr << "Failed to write to file"
                 ": error "
              << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  result = ceph_ll_close(mount.get(), fhp_test_file);
  if (result) {
    std::cerr << "Failed to close file " << file_name << ": error " << -result
              << " (" << ::strerror(-result) << ")" << std::endl;
  }

  std::shared_ptr<Inode> scoped_test_file_inode(
      test_file_inode,
      [mount](Inode* inode) { ceph_ll_put(mount.get(), inode); });

  result = ceph_ll_setxattr(mount.get(), test_file_inode, xattr_name.c_str(),
                            xattr_value.c_str(), xattr_value.size(), 0,
                            ceph_mount_perms(mount.get()));
  if (result) {
    std::cerr << "Failed to set xattr " << xattr_name << ": error " << -result
              << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  result = ceph_ll_getxattr(mount.get(), test_file_inode, xattr_name.c_str(),
                            nullptr, 0, ceph_mount_perms(mount.get()));
  if (result < 0) {
    std::cerr << "Failed to get length of active dir's xattr " << xattr_name
              << ": error " << -result << " (" << ::strerror(-result) << ")"
              << std::endl;
    return result;
  }

#if 1
  std::string create_snap_cmd = "ceph fs subvolume snapshot create " + volume +
                                " " + sub_volume + " " + snap_name;
  std::cout << create_snap_cmd << std::endl;
  result = system(create_snap_cmd.c_str());
  if (result) {
    std::cerr << "Failed to create snapshot " << snap_name << ": error "
              << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

#else
  result = ceph_mksnap(mount.get(), fs_path.c_str(), snap_name.c_str(), 0755,
                       nullptr, 0);
  if (result) {
    std::cerr << "Failed to create snapshot " << snap_name << ": error "
              << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  std::string snap_path = snap_dir + "/" + snap_name;

#endif

  result = ceph_ll_rmdir(mount.get(), test_dir_inode, sub_dir_name.c_str(),
                         ceph_mount_perms(mount.get()));
  if (result) {
    std::cerr << "Failed to rmdir"
              << ": error " << -result << " (" << ::strerror(-result) << ")"
              << std::endl;
    return result;
  }

  result = ceph_ll_rmdir(mount.get(), inode_fs, dir_name.c_str(),
                         ceph_mount_perms(mount.get()));
  if (result) {
    std::cerr << "Failed to rmdir"
              << ": error " << -result << " (" << ::strerror(-result) << ")"
              << std::endl;
    return result;
  }

  // result = ceph_ll_unlink(mount.get(), inode_fs, file_name.c_str(),
  // ceph_mount_perms(mount.get())); if (result) {
  //   std::cerr << "Failed to unlink file" ": error " << -result << " (" <<
  //   ::strerror(-result) << ")" << std::endl; return result;
  // }

  // result = ceph_ll_setxattr(mount.get(), test_file_inode, xattr_name.c_str(),
  // xattr_value.c_str(), xattr_value.size(), 0, ceph_mount_perms(mount.get()));
  // if (result) {
  //     std::cerr << "Failed to set xattr " << xattr_name << ": error " <<
  //     -result << " (" << ::strerror(-result) << ")" << std::endl; return
  //     result;
  // }

  return result;
}

int main(int, char**) {
  std::shared_ptr<ceph_mount_info> mount;
  std::shared_ptr<UserPerm> user_perms;

  int result = Mount(mount, user_perms);
  if (result) {
    std::cerr << "Failed to mount ceph: error " << -result << " ("
              << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  struct ceph_statx dir_sb;
  struct ceph_statx sub_dir_sb;
  struct ceph_statx file_sb;

  result = prepare(mount, dir_sb, sub_dir_sb, file_sb);
  if (result) {
    return result;
  }

  // Example: _test-snapshot-snap_1099511690785
  struct ceph_statx sub_volume_sb;
  Inode* sub_volume_inode = nullptr;

  result =
      ceph_ll_walk(mount.get(), sub_volume_path.c_str(), &sub_volume_inode,
                   &sub_volume_sb, CEPH_STATX_ALL_STATS, 0, user_perms.get());
  if (result) {
    std::cerr << "Failed to walk ceph path " << sub_volume_path << ": error "
              << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  std::shared_ptr<Inode> scoped_sub_volume_inode(
      sub_volume_inode,
      [mount](Inode* inode) { ceph_ll_put(mount.get(), inode); });

  const std::string snap_dir_name =
      "_" + snap_name + "_" + std::to_string(sub_volume_sb.stx_ino);
  std::string snap_path = snap_dir + "/" + snap_dir_name;
  struct ceph_statx snap_sb;

  result = ceph_statx(mount.get(), snap_path.c_str(), &snap_sb,
                      CEPH_STATX_ALL_STATS, 0);
  if (result) {
    std::cerr << "Failed to statx snapshot path " << snap_path << ": error "
              << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  snap_info snap_info;

  result = ceph_get_snap_info(mount.get(), snap_path.c_str(), &snap_info);
  if (result) {
    std::cerr << "Failed to get snap info of snapshot path " << snap_path
              << ": error " << -result << " (" << ::strerror(-result) << ")"
              << std::endl;
    return result;
  }

  const uint64_t snap_id = snap_info.id;
  const vinodeno vivo_dir = {dir_sb.stx_ino, snap_id};
  const vinodeno vivo_sub_dir = {sub_dir_sb.stx_ino, snap_id};
  const vinodeno vivo_file = {file_sb.stx_ino, snap_id};

  Inode* test_dir_inode_snap = nullptr;
  result = ceph_ll_lookup_vino(mount.get(), vivo_dir, &test_dir_inode_snap);
  if (result) {
    std::cerr << "Failed to lookup inode of directory {"
              << std::to_string(vivo_dir.ino.val) << ", "
              << std::to_string(vivo_dir.snapid.val) << "} in snapshot: error "
              << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  std::shared_ptr<Inode> scoped_test_dir_inode_snap(
      test_dir_inode_snap,
      [mount](Inode* inode) { ceph_ll_put(mount.get(), inode); });

  Inode* test_sub_dir_inode_snap = nullptr;
  result = ceph_ll_lookup_vino(mount.get(), vivo_sub_dir, &test_sub_dir_inode_snap);
  if (result) {
    std::cerr << "Failed to lookup inode of sub directory {"
              << std::to_string(vivo_sub_dir.ino.val) << ", "
              << std::to_string(vivo_sub_dir.snapid.val) << "} in snapshot: error "
              << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  std::shared_ptr<Inode> scoped_test_sub_dir_inode_snap(
      test_sub_dir_inode_snap,
      [mount](Inode* inode) { ceph_ll_put(mount.get(), inode); });

  Inode* test_file_inode_snap = nullptr;
  result = ceph_ll_lookup_vino(mount.get(), vivo_file, &test_file_inode_snap);
  if (result) {
    std::cerr << "Failed to lookup inode of file {"
              << std::to_string(vivo_file.ino.val) << ", "
              << std::to_string(vivo_file.snapid.val) << "} in snapshot: error "
              << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  std::shared_ptr<Inode> scoped_test_file_inode_snap(
      test_file_inode_snap,
      [mount](Inode* inode) { ceph_ll_put(mount.get(), inode); });

#if 0
    {
      const std::string dir_snap_path = snap_path + "/" + dir_name;
      struct ceph_statx dir_sb_snap;

      result = ceph_statx(mount.get(), dir_snap_path.c_str(), &dir_sb_snap, CEPH_STATX_ALL_STATS, 0);
      if (result) {
          std::cerr << "Failed to statx directory in snapshot " << dir_snap_path << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
          return result;
      }
    }
    {
      const std::string file_snap_path = snap_path + "/" + file_name;
      struct ceph_statx file_sb_snap;

      result = ceph_statx(mount.get(), file_snap_path.c_str(), &file_sb_snap, CEPH_STATX_ALL_STATS, 0);
      if (result) {
          std::cerr << "Failed to statx file in snapshot " << file_snap_path << ": error " << -result << " (" << ::strerror(-result) << ")" << std::endl;
          return result;
      }
    }
#elif 0
  struct ceph_statx sb_snap_dir;
  result = ceph_statx(mount.get(), snap_dir.c_str(), &sb_snap_dir,
                      CEPH_STATX_ALL_STATS, 0);
  if (result) {
    std::cerr << "Failed to statx snapshot dir " << snap_dir << ": error "
              << -result << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  Inode* inode_snap_dir = nullptr;
  result = ceph_ll_lookup_vino(
      mount.get(), {sb_snap_dir.stx_ino, sb_snap_dir.stx_dev}, &inode_snap_dir);
  if (result) {
    std::cerr << "Failed to lookup inode of .snap directory error " << -result
              << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  Inode* inode_snap_path = nullptr;
  struct ceph_statx sb_snap_path;
  result = ceph_ll_lookup(mount.get(), inode_snap_dir, snap_dir_name.c_str(),
                          &inode_snap_path, &sb_snap_path, CEPH_STATX_INO, 0,
                          user_perms.get());
  if (result) {
    std::cerr << "Failed to lookup inode of snapshot path " << snap_dir_name
              << ": error " << -result << " (" << ::strerror(-result) << ")"
              << std::endl;
    return result;
  }

  std::shared_ptr<Inode> scoped_inode_snap_path(
      inode_snap_path,
      [mount](Inode* inode) { ceph_ll_put(mount.get(), inode); });

  {
    Inode* inode_ = nullptr;
    struct ceph_statx sb_;

    result = ceph_ll_lookup(mount.get(), inode_snap_path, dir_name.c_str(),
                            &inode_, &sb_, CEPH_STATX_INO, 0, user_perms.get());
    if (result) {
      std::cerr << "Failed to lookup inode of snapshot sub directory "
                << dir_name << ": error " << -result << " ("
                << ::strerror(-result) << ")" << std::endl;
      return result;
    }

    std::shared_ptr<Inode> scoped_inode_(
        inode_, [mount](Inode* inode) { ceph_ll_put(mount.get(), inode); });
  }

  {
    Inode* inode_ = nullptr;
    struct ceph_statx sb_;

    result = ceph_ll_lookup(mount.get(), inode_snap_path, file_name.c_str(),
                            &inode_, &sb_, CEPH_STATX_INO, 0, user_perms.get());
    if (result) {
      std::cerr << "Failed to lookup inode of snapshot file " << dir_name
                << ": error " << -result << " (" << ::strerror(-result) << ")"
                << std::endl;
      return result;
    }

    std::shared_ptr<Inode> scoped_inode_(
        inode_, [mount](Inode* inode) { ceph_ll_put(mount.get(), inode); });
  }
#elif 1

  std::shared_ptr<Inode> scoped_inode_the_snap;
  std::string name_the_snap;

  auto ecb = [snap_id, &scoped_inode_the_snap, &name_the_snap](const std::string& name,
                                               const struct ceph_statx& sb,
                                               std::shared_ptr<Inode> inode) {
    if (sb.stx_dev == snap_id) {
      scoped_inode_the_snap = inode;
      name_the_snap = name;
      return false;
    }
    return true;
  };

  // Access directory
  const vinodeno vivo_live_dir = {dir_sb.stx_ino, dir_sb.stx_dev};

  Inode* inode_live_dir = nullptr;
  result = ceph_ll_lookup_vino(mount.get(), vivo_live_dir, &inode_live_dir);
  if (result) {
    std::cerr << "Failed to lookup inode of the live directory " << -result
              << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  std::shared_ptr<Inode> scoped_inode_live_dir(
      inode_live_dir,
      [mount](Inode* inode) { ceph_ll_put(mount.get(), inode); });

  {
    struct ceph_statx sb;

    result = ceph_ll_getattr(mount.get(), inode_live_dir, &sb, CEPH_STATX_MODE,
                             0, user_perms.get());
    if (result < 0) {
      std::cerr << "Failed to stat deleted directory"
                << ": error " << -result << " (" << ::strerror(-result) << ")"
                << std::endl;
    }

    if (not S_ISDIR(sb.stx_mode)) {
      std::cerr << "Failed to read type of deleted directory" << std::endl;
    }
  }

  Inode* inode_snap_the_dir = nullptr;
  struct ceph_statx sb;

  result =
      ceph_ll_lookup(mount.get(), inode_live_dir, ".snap", &inode_snap_the_dir,
                     &sb, CEPH_STATX_INO, 0, user_perms.get());
  if (result) {
    std::cerr << "Filed to lookup .snap in live directory"
              << ": error " << -result << " (" << ::strerror(-result) << ")"
              << std::endl;
    return result;
  }

  std::shared_ptr<Inode> scoped_inode_snap_the_dir(
      inode_snap_the_dir,
      [mount](Inode* inode) { ceph_ll_put(mount.get(), inode); });

  result = ReadDir(mount, scoped_inode_snap_the_dir, ecb);
  if (not scoped_inode_the_snap) {
    std::cerr << "Failed to find snapshot in the .snap"
              << ": error " << -result << " (" << ::strerror(-result) << ")"
              << std::endl;
    return result;
  }

  // Lookup snapshot .snap below is useless
  Inode* inode_dir_target = nullptr;
  result = ceph_ll_lookup(mount.get(), scoped_inode_snap_the_dir.get(),
  name_the_snap.c_str(), &inode_dir_target, &sb, CEPH_STATX_INO, 0,
  user_perms.get()); if (result) {
    std::cerr << "Failed to look up " << name_the_snap << ": error " <<
    -result << " (" << ::strerror(-result) << ")" << std::endl; return
    result;
  }

  std::shared_ptr<Inode> scoped_inode_dir_target(inode_dir_target,
  [mount](Inode* inode) {
    ceph_ll_put(mount.get(), inode);
  });

#elif 0
  struct ceph_statx sb_fs;
  Inode* inode_fs = nullptr;

  result = ceph_ll_walk(mount.get(), fs_path.c_str(), &inode_fs, &sb_fs,
                        CEPH_STATX_ALL_STATS, 0, user_perms.get());
  if (result) {
    std::cerr << "Failed to walk ceph path " << fs_path << ": error " << -result
              << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  Inode* inode_snap_dir = nullptr;
  struct ceph_statx sb_snap_dir;

  result = ceph_ll_lookup(mount.get(), inode_fs, ".snap", &inode_snap_dir,
                          &sb_snap_dir, CEPH_STATX_INO, 0, user_perms.get());
  if (result) {
    std::cerr << "Failed to lookup inode of .snap directory: error " << -result
              << " (" << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  auto scoped_inode_snap_dir = std::shared_ptr<Inode>(
      inode_snap_dir,
      [mount](Inode* inode) { ceph_ll_put(mount.get(), inode); });

  std::shared_ptr<Inode> scoped_inode_parent;

  auto ecb = [snap_id, &scoped_inode_parent](const std::string& name,
                                             const struct ceph_statx& sb,
                                             std::shared_ptr<Inode> inode) {
    if (sb.stx_dev == snap_id) {
      scoped_inode_parent = inode;
      return false;
    }
    return true;
  };

  result = ReadDir(mount, scoped_inode_snap_dir, ecb);
  if (not scoped_inode_parent) {
    std::cerr << "Failed to find snapshot inode"
              << ": error " << -result << " (" << ::strerror(-result) << ")"
              << std::endl;
    return result;
  }

  std::shared_ptr<Inode> scoped_inode_my;
  std::deque<std::shared_ptr<Inode> > inode_queue;

  while (true) {
    bool done = false;

    auto ecb = [&dir_sb, &scoped_inode_my, &inode_queue](
                   const std::string& name, const struct ceph_statx& sb,
                   std::shared_ptr<Inode> inode) {
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
  // const char* new_xattr_name = "ceph.snap.btime";
  std::string xattr_read;
  const char* new_xattr_name = xattr_name.c_str();

  result = ceph_ll_getxattr(mount.get(), test_dir_inode_snap, new_xattr_name,
                            nullptr, 0, user_perms.get());
  if (result < 0) {
    std::cerr << "Failed to get length of snapshot dir's xattr "
              << new_xattr_name << ": error " << -result << " ("
              << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  xattr_read.resize(result + 1, '\0');

  result = ceph_ll_getxattr(mount.get(), test_dir_inode_snap, new_xattr_name,
                            xattr_read.data(), result, user_perms.get());
  if (result < 0) {
    std::cerr << "Failed to get snapshot dir's xattr " << new_xattr_name
              << ": error " << -result << " (" << ::strerror(-result) << ")"
              << std::endl;
    return result;
  }

  std::cerr << "xattr of dir in snapshot: " << snap_id << " is: " << xattr_read
            << std::endl;

  bool found_sub_dir = false;
  auto sub_dir_ecb = [&sub_dir_sb, &found_sub_dir](
                  const std::string& name, const struct ceph_statx& sb,
                  std::shared_ptr<Inode> inode) {
    bool next = true;

    std::cout << "searching snapshotted directory: " << name << std::endl;

    if (sb.stx_ino == sub_dir_sb.stx_ino) {
      found_sub_dir = true;
      next = false;
    } 

    return next;
  };

  result = ReadDir(mount, scoped_test_dir_inode_snap, sub_dir_ecb);
  if (not found_sub_dir) {
    std::cerr << "Failed to find sub-dir in snapshot"
              << ": error " << -result << " (" << ::strerror(-result) << ")"
              << std::endl;
    // return result;
  }

  result = ceph_ll_getxattr(mount.get(), test_sub_dir_inode_snap, new_xattr_name,
                            nullptr, 0, user_perms.get());
  if (result < 0) {
    std::cerr << "Failed to get length of snapshot sub-dir's xattr "
              << new_xattr_name << ": error " << -result << " ("
              << ::strerror(-result) << ")" << std::endl;
    // return result;
  } else {
    xattr_read.resize(result + 1, '\0');

    result = ceph_ll_getxattr(mount.get(), test_sub_dir_inode_snap, new_xattr_name,
                              xattr_read.data(), result, user_perms.get());
    if (result < 0) {
      std::cerr << "Failed to get snapshot sub-dir's xattr " << new_xattr_name
                << ": error " << -result << " (" << ::strerror(-result) << ")"
                << std::endl;
      return result;
    }

    std::cerr << "xattr of sub-dir in snapshot: " << snap_id << " is: " << xattr_read
              << std::endl;
  }

  result = ceph_ll_getxattr(mount.get(), test_file_inode_snap, new_xattr_name,
                            nullptr, 0, user_perms.get());
  if (result < 0) {
    std::cerr << "Failed to get length of snapshot file's xattr "
              << new_xattr_name << ": error " << -result << " ("
              << ::strerror(-result) << ")" << std::endl;
    return result;
  }

  xattr_read.resize(result + 1, '\0');

  result = ceph_ll_getxattr(mount.get(), test_file_inode_snap, new_xattr_name,
                            xattr_read.data(), result, user_perms.get());
  if (result < 0) {
    std::cerr << "Failed to get snapshot file's xattr " << new_xattr_name
              << ": error " << -result << " (" << ::strerror(-result) << ")"
              << std::endl;
    return result;
  }

  std::cerr << "xattr of file in snapshot: " << snap_id << " is: " << xattr_read
            << std::endl;

  Fh* fh_snap = nullptr;
  result = ceph_ll_open(mount.get(), test_file_inode_snap, O_RDONLY, &fh_snap,
                        user_perms.get());
  if (result < 0) {
    std::cerr << "Failed to open snapshot file"
              << ": error " << -result << " (" << ::strerror(-result) << ")"
              << std::endl;
    return result;
  }

  std::string buf(10, '\0');
  result = ceph_ll_read(mount.get(), fh_snap, 0, 9, buf.data());
  if (result != 9) {
    std::cerr << "Failed to read snapshot file"
              << ": error " << -result << " (" << ::strerror(-result) << ")"
              << std::endl;
    return result;
  }

  result = ceph_ll_close(mount.get(), fh_snap);
  if (result) {
    std::cerr << "Failed to close snapshot file"
              << ": error " << -result << " (" << ::strerror(-result) << ")"
              << std::endl;
    return result;
  }

  std::cerr << "content of file in snapshot: " << buf << std::endl;

  return 0;
}
