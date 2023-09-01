import os
import signal
from ducktape.utils.util import wait_until
from ducktape.services.service import Service
from ducktape.cluster.remoteaccount import RemoteCommandError
class PD(Service):
    ROOT = "/home/ducker/pd"

    def __init__(self, context, num_nodes):
        super(PD, self).__init__(context, num_nodes)

    def restart_cluster(self):
        for node in self.nodes:
            self.restart_node(node)
        wait_until(lambda: self.ready(), timeout_sec=30, err_msg="PD node failed to restart")

    def restart_node(self, node):
        """Restart the given node."""
        self.stop_node(node)
        self.start_node(node)

    def start_node(self, node):
        idx = self.idx(node)
        self.logger.info("Starting PD node %d on %s", idx, node.account.hostname)
        node.account.ssh("mkdir -p %s" % PD.ROOT)
        cmd = self.start_cmd(node)
        node.account.ssh(cmd)
        self.hostname = node.account.hostname

    def ready(self):
        for node0 in self.nodes:
            if self.listening(node0) == False:
                return False
        return True

    def listening(self, node):
        try:
            port = 12378
            cmd = "nc -z %s %s" % (node.account.hostname, port)
            node.account.ssh_output(cmd, allow_fail=False)
            self.logger.debug("PD started accepting connections at: '%s:%s')", node.account.hostname, port)
            return True
        except (RemoteCommandError, ValueError) as e:
            return False

    def start_cmd(self, node):
        cmd = "cd " + PD.ROOT + ";"
        cmd += "export PD_PEERURLS=http://0.0.0.0:12380;"
        cmd += "export PD_CLIENTURLS=http://0.0.0.0:12379;"
        cmd += "export PD_PDADDR=0.0.0.0:12378;"
        cmd += "export PD_ADVERTISEPEERURLS=http://" + node.account.hostname + ":12380;"
        cmd += "export PD_ADVERTISECLIENTURLS=http://" + node.account.hostname + ":12379;"
        cmd += "export PD_ADVERTISEPDADDR=" + node.account.hostname + ":12378;"
        init_cmd = ""
        for node0 in self.nodes:
            init_cmd += "pd-" +  node0.account.hostname + "=http://" + node0.account.hostname + ":12380,"
        init_cmd = init_cmd[:-1]
        init_cmd = "export PD_INITIALCLUSTER=\"" + init_cmd + "\";"
        cmd += init_cmd
        cmd += "export PD_ETCD_INITIALCLUSTERTOKEN=pd-cluster;"
        cmd += "pd &>> pd.log &"
        return cmd


    def pids(self, node):
        try:
            # cmd = "ps -ax | grep pd | awk '{print $1}'"
            cmd = "pgrep pd"
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError) as e:
            return []

    def alive(self, node):
        return len(self.pids(node)) > 0

    def kill_node(self, node):
        idx = self.idx(node)
        self.logger.info("Killing %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
        self.signal_node(node, signal.SIGKILL)
        wait_until(lambda: not self.alive(node), timeout_sec=5, err_msg="Timed out waiting for PD to be killed.")

    def stop_node(self, node):
        idx = self.idx(node)
        self.logger.info("Stopping %s node %d on %s" % (type(self).__name__, idx, node.account.hostname))
        self.signal_node(node)
        wait_until(lambda: not self.alive(node), timeout_sec=30, err_msg="Timed out waiting for PD to stop.")

    def signal_node(self, node, sig=signal.SIGTERM):
        pids = self.pids(node)
        for pid in pids:
            # print("Kill pid: ", pid)
            node.account.signal(pid, sig)

    def clean(self):
        for node in self.nodes:
            self.clean_node(node)

    def clean_node(self, node):
        # self.stop_node(node)
        self.kill_node(node)
        node.account.ssh("sudo rm -rf -- %s" % PD.ROOT, allow_fail=True)

    def get_hostname(self):
        return self.hostname

    def start(self):
        Service.start(self)
        wait_until(lambda: self.ready(), timeout_sec=30, err_msg="PD node failed to start")
