import os
import os.path
import errno
import functools
import traceback
import pprint
import threading
from zstacklib.utils import log
import zstacklib.utils.shell as shell

import zstacklib.utils.lichbd_version_const as lichbdconst

logger = log.get_logger(__name__)
class LichbdVersionBase(object):
    def __init__(self):
        super(LichbdVersionBase, self).__init__()
        self.LICHBD_CMD_POOL_CREATE = 'lichbd mkpool'
        self.LICHBD_CMD_POOL_LS = 'lichbd lspools'
        self.LICHBD_CMD_POOL_RM = 'lichbd rmpool'
        self.LICHBD_CMD_VOL_CREATE = 'lichbd create'
        self.LICHBD_CMD_VOL_COPY = 'lichbd copy'
        self.LICHBD_CMD_VOL_IMPORT = 'lichbd import'
        self.LICHBD_CMD_VOL_EXPORT = 'lichbd export'
        self.LICHBD_CMD_VOL_RM = 'lichbd rm'
        self.LICHBD_CMD_VOL_MV = 'lichbd mv'
        self.LICHBD_CMD_VOL_INFO = 'lichbd info'
        self.LICHBD_CMD_SNAP_CREATE = 'lichbd snap create'
        self.LICHBD_CMD_SNAP_LS = 'lichbd snap ls'
        self.LICHBD_CMD_SNAP_RM = 'lichbd snap remove'
        self.LICHBD_CMD_SNAP_CLONE = 'lichbd clone'
        self.LICHBD_CMD_SNAP_ROLLBACK = 'lichbd snap rollback'
        self.LICHBD_CMD_SNAP_PROTECT = 'lichbd snap protect'
        self.LICHBD_CMD_SNAP_UNPROTECT = 'lichbd snap unprotect'

    def get_pool_create_cmd(self, path=None, protocol=None):
        return '%s %s -p %s 2>/dev/null'%(self.LICHBD_CMD_POOL_CREATE, path, protocol)

    def get_pool_ls_cmd(self, protocol=None):
        return '%s -p %s 2>/dev/null'% (self.LICHBD_CMD_POOL_LS, protocol)

    def get_pool_rm_cmd(self, path=None, protocol=None):
        return '%s %s -p %s 2>/dev/null'% (self.LICHBD_CMD_POOL_RM, path, protocol)

    def get_vol_create_cmd(self, path=None, size=None, protocol=None):
        return '%s %s --size %s -p %s 2>/dev/null'% (self.LICHBD_CMD_VOL_CREATE, path, size, protocol)

    def get_vol_copy_cmd(self, src_path=None, dst_path=None,protocol=None):
        return '%s %s %s -p %s 2>/dev/null'% (self.LICHBD_CMD_VOL_COPY, src_path, dst_path, protocol)

    def get_vol_import_cmd(self, src_path=None, dst_path=None, protocol=None):
        return '%s %s %s -p %s 2>/dev/null'% (self.LICHBD_CMD_VOL_IMPORT, src_path, dst_path, protocol)

    def get_vol_import_to_stdin_cmd(self, dst_path=None, protocol=None):
        return '%s - %s -p %s' % (self.LICHBD_CMD_VOL_IMPORT, dst_path, protocol)

    def get_vol_export_cmd(self, src_path=None, dst_path=None, protocol=None):
        return '%s %s %s -p %s 2>/dev/null'% (self.LICHBD_CMD_VOL_EXPORT, src_path, dst_path, protocol)

    def get_vol_rm_cmd(self, path=None, protocol=None):
        return '%s %s -p %s 2>/dev/null'% (self.LICHBD_CMD_VOL_RM, path, protocol)

    def get_vol_mv_cmd(self, src_path=None, dst_path=None, protocol=None):
        return '%s %s %s -p %s 2>/dev/null'% (self.LICHBD_CMD_VOL_MV, src_path, dst_path, protocol)

    def get_vol_info_cmd(self, path=None, protocol=None):
        return '%s %s -p %s'% (self.LICHBD_CMD_VOL_INFO, path, protocol)

    def get_snap_create_cmd(self, snap_path=None, protocol=None):
        return '%s %s -p %s'% (self.LICHBD_CMD_SNAP_CREATE, snap_path, protocol)

    def get_snap_ls_cmd(self, image_path=None, protocol=None):
        return '%s %s -p %s 2>/dev/null'% (self.LICHBD_CMD_SNAP_LS, image_path, protocol)

    def get_snap_rm_cmd(self, snap_path=None, protocol=None):
        return '%s %s -p %s'% (self.LICHBD_CMD_SNAP_RM, snap_path, protocol)

    def get_snap_clone_cmd(self, src=None, dst=None, protocol=None):
        return '%s %s %s -p %s'% (self.LICHBD_CMD_SNAP_CLONE, src, dst, protocol)

    def get_snap_rollback_cmd(self, snap_path=None, protocol=None):
        return '%s %s -p %s'% (self.LICHBD_CMD_SNAP_ROLLBACK, snap_path, protocol)

    def get_snap_protect_cmd(self, snap_path=None, protocol=None):
        return '%s %s -p %s'% (self.LICHBD_CMD_SNAP_PROTECT, snap_path, protocol)

    def get_snap_unprotect_cmd(self, snap_path=None, protocol=None):
        return '%s %s -p %s'% (self.LICHBD_CMD_SNAP_UNPROTECT, snap_path, protocol)

    def get_pool_path(self):
        return ''

    def __call_shellcmd(self, cmd, exception=False, workdir=None):
        shellcmd = shell.ShellCmd(cmd, workdir)
        shellcmd(exception)
        return shellcmd

    def call_try(self, cmd, exception=False, workdir=None, try_num=None):
        if try_num is None:
            try_num = 10

        shellcmd = None
        for i in range(try_num):
            shellcmd = self.__call_shellcmd(cmd, False, workdir)
            if shellcmd.return_code == 0 or shellcmd.return_code == errno.EEXIST:
                break

            time.sleep(1)

        return shellcmd

    def lichbd_cluster_stat(self):
        shellcmd = self.call_try('lich stat --human-unreadable 2>/dev/null')
        if shellcmd.return_code != 0:
            raise_exp(shellcmd)

        return shellcmd.stdout

    def lichbd_get_used(self):
        o = self.lichbd_cluster_stat()
        for l in o.split("\n"):
            if 'used:' in l:
                used = long(l.split("used:")[-1])
                return used

        raise shell.ShellError('lichbd_get_used')

    def lichbd_get_capacity(self):
        try:
            o = self.lichbd_cluster_stat()
        except Exception, e:
            raise shell.ShellError('lichbd_get_capacity')

        total = 0
        used = 0
        for l in o.split("\n"):
            if 'capacity:' in l:
                total = long(l.split("capacity:")[-1])
            elif 'used:' in l:
                used = long(l.split("used:")[-1])

        return total, used

    def is_support_snap_tree(self):
        return False

    def get_qemu_path(self):
        return "/opt/fusionstack/qemu/bin/qemu-system-x86_64"

    def get_qemu_img_path(self):
        return "/opt/fusionstack/qemu/bin/qemu-img"

#359 is the internalid of Q4 2016
class LichbdVersionBefore375(LichbdVersionBase):
    def __init__(self):
        super(LichbdVersionBefore375, self).__init__()

class LichbdVersionBefore403(LichbdVersionBase):

    def __init__(self):
        super(LichbdVersionBefore403, self).__init__()
        self.LICHBD_CMD_POOL_CREATE = 'lichbd pool create'
        self.LICHBD_CMD_POOL_LS = 'lichbd pool ls'
        self.LICHBD_CMD_POOL_RM = 'lichbd pool rm'
        self.LICHBD_CMD_VOL_CREATE = 'lichbd vol create'
        self.LICHBD_CMD_VOL_COPY = 'lichbd vol copy'
        self.LICHBD_CMD_VOL_IMPORT = 'lichbd vol import'
        self.LICHBD_CMD_VOL_EXPORT = 'lichbd vol export'
        self.LICHBD_CMD_VOL_RM = 'lichbd vol rm'
        self.LICHBD_CMD_VOL_MV = 'lichbd vol mv'
        self.LICHBD_CMD_VOL_INFO = 'lichbd vol info'
        self.LICHBD_CMD_SNAP_RM = 'lichbd snap rm'
        self.LICHBD_CMD_SNAP_CLONE = 'lichbd snap clone'


#403 lich/dev_cmnet_bin117
class LichbdVersionLatest(LichbdVersionBase):
    def __init__(self):
        super(LichbdVersionLatest, self).__init__()
        self.POOL_PATH = '/default/nbd/'
        self.LICHBD_CMD_POOL_CREATE = 'lichbd dir create'
        self.LICHBD_CMD_POOL_LS = 'lichbd dir ls'
        self.LICHBD_CMD_POOL_RM = 'lichbd dir rm'
        self.LICHBD_CMD_VOL_CREATE = 'lichbd vol create'
        self.LICHBD_CMD_VOL_COPY = 'lichbd vol copy'
        self.LICHBD_CMD_VOL_IMPORT = 'lichbd vol import'
        self.LICHBD_CMD_VOL_EXPORT = 'lichbd vol export'
        self.LICHBD_CMD_VOL_RM = 'lichbd vol rm'
        self.LICHBD_CMD_VOL_MV = 'lichbd vol mv'
        self.LICHBD_CMD_VOL_INFO = 'lichbd vol info -C full'
        self.LICHBD_CMD_SNAP_RM = 'lichbd snap rm'
        self.LICHBD_CMD_SNAP_CLONE = 'lichbd snap clone'

    def get_pool_create_cmd(self, path=None, protocol=None):
        return '%s %s%s 2>/dev/null' % (self.LICHBD_CMD_POOL_CREATE, self.POOL_PATH, path)

    def get_pool_ls_cmd(self, protocol=None):
        return '%s %s 2>/dev/null' % (self.LICHBD_CMD_POOL_LS, self.POOL_PATH)

    def get_pool_rm_cmd(self, path=None, protocol=None):
        return '%s %s%s 2>/dev/null' % (self.LICHBD_CMD_POOL_RM, self.POOL_PATH, path)

    def get_vol_create_cmd(self, path=None, size=None, protocol=None):
        return '%s %s%s --size %s 2>/dev/null' % (self.LICHBD_CMD_VOL_CREATE, self.POOL_PATH, path, size)

    def get_vol_copy_cmd(self, src_path=None, dst_path=None, protocol=None):
        return '%s %s%s %s%s 2>/dev/null' % (self.LICHBD_CMD_VOL_COPY, self.POOL_PATH, src_path, self.POOL_PATH, dst_path)

    def get_vol_import_cmd(self, src_path=None, dst_path=None, protocol=None):
        return '%s %s%s %s%s 2>/dev/null' % (self.LICHBD_CMD_VOL_IMPORT, self.POOL_PATH, src_path, self.POOL_PATH, dst_path)

    def get_vol_import_to_stdin_cmd(self, dst_path=None, protocol=None):
        return '%s - %s%s' % (self.LICHBD_CMD_VOL_IMPORT, self.POOL_PATH, dst_path)

    def get_vol_export_cmd(self, src_path=None, dst_path=None, protocol=None):
        return '%s %s%s %s%s 2>/dev/null' % (self.LICHBD_CMD_VOL_EXPORT, self.POOL_PATH, src_path, self.POOL_PATH, dst_path)

    def get_vol_rm_cmd(self, path=None, protocol=None):
        return '%s %s%s 2>/dev/null' % (self.LICHBD_CMD_VOL_RM, self.POOL_PATH, path)

    def get_vol_mv_cmd(self, src_path=None, dst_path=None, protocol=None):
        return '%s %s%s %s%s  2>/dev/null' % (self.LICHBD_CMD_VOL_MV, self.POOL_PATH, src_path, self.POOL_PATH, dst_path)

    def get_vol_info_cmd(self, path=None, protocol=None):
        return '%s %s%s ' % (self.LICHBD_CMD_VOL_INFO, self.POOL_PATH, path)

    def get_snap_create_cmd(self, snap_path=None, protocol=None):
        return '%s %s%s' % (self.LICHBD_CMD_SNAP_CREATE, self.POOL_PATH, snap_path)

    def get_snap_ls_cmd(self, image_path=None, protocol=None):
        return '%s %s%s 2>/dev/null' % (self.LICHBD_CMD_SNAP_LS, self.POOL_PATH, image_path)

    def get_snap_rm_cmd(self, snap_path=None, protocol=None):
        return '%s %s%s' % (self.LICHBD_CMD_SNAP_RM, self.POOL_PATH, snap_path)

    def get_snap_clone_cmd(self, src=None, dst=None, protocol=None):
        return '%s %s%s %s%s' % (self.LICHBD_CMD_SNAP_CLONE, self.POOL_PATH, src, self.POOL_PATH, dst)

    def get_snap_rollback_cmd(self, snap_path=None, protocol=None):
        return '%s %s%s' % (self.LICHBD_CMD_SNAP_ROLLBACK, self.POOL_PATH, snap_path)

    def get_snap_protect_cmd(self, snap_path=None, protocol=None):
        return '%s %s%s' % (self.LICHBD_CMD_SNAP_PROTECT, self.POOL_PATH, snap_path)

    def get_snap_unprotect_cmd(self, snap_path=None, protocol=None):
        return '%s %s%s' % (self.LICHBD_CMD_SNAP_UNPROTECT, self.POOL_PATH, snap_path)

    def get_pool_path(self):
        return self.POOL_PATH

    def lichbd_get_used(self):
        o = self.lichbd_cluster_stat()
        for l in o.split("\n"):
            if 'default:' in l:
                used = long(l.split("default:")[-1].split("/")[0])
                return used

        raise shell.ShellError('lichbd_get_used')

    def lichbd_get_capacity(self):
        try:
            o = self.lichbd_cluster_stat()
        except Exception, e:
            logger.warn("lichbd_get_capacity exception %s" % e.message)
            raise shell.ShellError(str(e))

        total = 0
        used = 0
        for l in o.split("\n"):
            if 'default:' in l:
                total = long(l.split("default:")[-1].split("/")[-1])
                used = long(l.split("default:")[-1].split("/")[0])
        return total, used

    def is_support_snap_tree(self):
        return True

    def get_qemu_path(self):
        return "/opt/fusionstack/qemu-2.6/bin/qemu-system-x86_64"

    def get_qemu_img_path(self):
        return "/opt/fusionstack/qemu-2.6/bin/qemu-img"

def get_lichbd_version_class(lichbd_version):
    version_class = None
    logger.warn("lichbd version is %s" % lichbd_version)
    if(lichbd_version < lichbdconst.LichbdVersionConst.LICHBD_VERSION_Q4_2016):
        version_class = LichbdVersionBefore375()
    elif(lichbd_version < lichbdconst.LichbdVersionConst.LICHBD_VERSION_Q4_2017):
        version_class = LichbdVersionBefore403()
    else:
        version_class = LichbdVersionLatest()
    return version_class
