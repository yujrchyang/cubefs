#!/usr/bin/env python3
import os
import re
import sys
import json
import argparse
import subprocess
import shutil
import time
import signal
import glob
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
from pathlib import Path
from typing import Union, Any, List, Dict
from abc import ABC, abstractmethod


class CommandExecutor:
    """Command execution tool class, encapsulating subprocess calls and error handling"""

    @staticmethod
    def _get_run_kwargs(capture_output: bool) -> Dict[str, Any]:
        if sys.version_info >= (3, 7):
            text_mode = {"text": True}
        else:
            text_mode = {"universal_newlines": True}
        kwargs = {
            "shell": False,
            "stdout": subprocess.PIPE if capture_output else None,
            "stderr": subprocess.PIPE if capture_output else None,
            **text_mode
        }
        return kwargs

    @staticmethod
    def run(command: List[str], capture_output: bool = True) -> str:
        """
        Execute a shell command and return its standard output.
        Raises CalledProcessError if the command returns a non-zero exit status.
        """
        try:
            result = subprocess.run(
                command,
                check=True,
                **CommandExecutor._get_run_kwargs(capture_output)
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            print(f"failed to exec : {command} - {e.stderr}", file=sys.stderr)
            sys.exit(1)

    @staticmethod
    def run_raw(command: List[str]) -> subprocess.CompletedProcess:
        """
        Execute a shell command directly and return the raw result without exception handling.
        Always captures both stdout and stderr.
        """
        return subprocess.run(
            command,
            check=False,
            **CommandExecutor._get_run_kwargs(True)
        )

    @staticmethod
    def run_foreground_daemon(command: List[str], extra_env: Dict[str, str]) -> None:
        # Start the daemon process in the foreground
        try:
            env = os.environ.copy()
            if extra_env:
                env.update(extra_env)

            subprocess.run(
                command,
                stdout=sys.stdout,  # Output to container standard output
                stderr=sys.stderr,  # Errors are output to the container's standard error
                env=env,            # Specify more env
                check=True          # Throws an exception when the daemon exits
            )
        except subprocess.CalledProcessError as e:
            print(f"the daemon process exited abnormally, error code : {e.returncode}")
            sys.exit(e.returncode)
        except Exception as e:
            print(f"failed to start the daemon process : {str(e)}")
            sys.exit(1)

    @staticmethod
    def run_background_daemon(command: List[str], logfile: str):
        # Start the daemon process in the background
        pid = os.fork()
        if pid > 0:
            return pid

        # detach from the terminal
        os.setsid()
        # Second fork to prevent reacquisition of tty
        pid = os.fork()
        if pid > 0:
            os._exit(0)

        sys.stdout.flush()
        sys.stderr.flush()
        if logfile == "":
            logfile = "/dev/null"
        with open(logfile, 'ab', buffering=0) as log:
            os.dup2(log.fileno(), sys.stdout.fileno())
            os.dup2(log.fileno(), sys.stderr.fileno())
        with open('/dev/null', 'rb') as f:
            os.dup2(f.fileno(), sys.stdin.fileno())

        os.execvp(command[0], command)
        os._exit(255)

    @staticmethod
    def run_http_get_json(url: str, timeout=5) -> Union[Dict[str, Any], List[Any]]:
        try:
            with urlopen(url, timeout=timeout) as response:
                if response.status != 200:
                    return {}
                body = response.read().decode('utf-8')
                return json.loads(body)
        except (URLError, HTTPError, TimeoutError, ValueError, UnicodeDecodeError, AttributeError):
            pass
        return {}


class DirectoryManager:
    def __init__(self):
        VSTART_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
        self.bin_dir = os.path.abspath(os.path.join(VSTART_SCRIPT_DIR, '../../../build/bin/blobstore'))
        self.cfg_dir = os.path.abspath(os.path.join(VSTART_SCRIPT_DIR, 'config'))
        self.lib_dir = "/var/lib/blobstore"
        self.log_dir = "/var/log/blobstore"
        self.run_dir = "/var/run/blobstore"
        self.all_dirs = [self.lib_dir, self.log_dir, self.run_dir]

    @staticmethod
    def is_directory_empty(path) -> bool:
        if not os.path.exists(path):
            return True
        if not os.path.isdir(path):
            print(f"input path {path} is not a directory.")
            sys.exit(1)
        return len(os.listdir(path)) == 0

    @staticmethod
    def get_files_by_prefix(directory, prefix) -> List[str]:
        return [
            os.path.join(directory, filename)
            for filename in os.listdir(directory)
            if filename.startswith(prefix)
        ]

    @staticmethod
    def natural_sort_keys(s):
        return [int(text) if text.isdigit() else text.lower()
                for text in re.split(r'(\d+)', s)]

    def setup_directory(self) -> None:
        for dir in self.all_dirs:
            dir_path = Path(dir)
            dir_path.mkdir(parents=True, exist_ok=True)

    def remove_directory(self) -> None:
        for dir in self.all_dirs:
            dir_path = Path(dir)
            if dir_path.exists():
                shutil.rmtree(dir_path)


class ConfigFileManager:
    @staticmethod
    def get_json_data(json_path: str) -> Dict:
        try:
            with open(json_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"error: input json file {json_path} does not exist.")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"error: invalid json format of {json_path} : {str(e)}")
            sys.exit(1)
        except Exception as e:
            print(f"error: read json file {json_path} failed : {str(e)}")
            sys.exit(1)


class ServiceBase(ABC):
    def __init__(self, dir_manager: DirectoryManager):
        self.dir_manager = dir_manager
        self.command: List[str] = []
        self.logfile = ""
        self.name = ""

    def run_service(self) -> None:
        self._setup_service()
        self._start_service()
        self._check_service()

    def stop_service(self) -> None:
        self._setup_service_name()
        if self.name:
            print(f"stopping {self.name} ...")
            for pid in glob.glob("/proc/[0-9]*"):
                try:
                    if self.name in open(f"{pid}/cmdline").read().replace("\0", " "):
                        os.kill(int(pid.split("/")[-1]), signal.SIGKILL)
                except (FileNotFoundError, ProcessLookupError, PermissionError):
                    pass

    @abstractmethod
    def _setup_service(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def _check_service(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def _setup_service_name(self) -> None:
        raise NotImplementedError

    def _start_service(self) -> None:
        CommandExecutor.run_background_daemon(self.command, self.logfile)

class ServiceConsul(ServiceBase):
    def _setup_service(self) -> None:
        print("starting consul ...")
        self.command = ["/usr/bin/consul", "agent", "-dev", "-client", "0.0.0.0"]
        self.logfile = f"{self.dir_manager.log_dir}/consul-start.log"

    def _check_service(self) -> None:
        print("checking consul ...")
        url = "http://localhost:8500/v1/status/leader"
        while True:
            result = CommandExecutor.run_http_get_json(url)
            if isinstance(result, str) and result == "127.0.0.1:8300":
                print("consul started")
                break
            time.sleep(1)

    def _setup_service_name(self) -> None:
        self.name = "/usr/bin/consul"

class ServiceKafka(ServiceBase):
    def _setup_service(self) -> None:
        print("starting kafka ...")
        kafka_path = "/usr/bin/kafka_2.13-3.1.0"
        # format log directories
        formatted_file = "/tmp/kraft-combined-logs/meta.properties"
        if not os.path.exists(formatted_file):
            cluster_id = CommandExecutor.run([f"{kafka_path}/bin/kafka-storage.sh", "random-uuid"])
            if cluster_id.endswith('\n') or cluster_id.endswith('\r'):
                cluster_id = cluster_id[:-1]
            CommandExecutor.run([f"{kafka_path}/bin/kafka-storage.sh", "format", "-t", cluster_id, "-c", f"{kafka_path}/config/kraft/server.properties"])
        self.command = [f"{kafka_path}/bin/kafka-server-start.sh", "-daemon", f"{kafka_path}/config/kraft/server.properties"]

    def _check_service(self) -> None:
        print("checking kafka ...")
        kafka_path = "/usr/bin/kafka_2.13-3.1.0"
        cmd = [f"{kafka_path}/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092"]
        while True:
            res = CommandExecutor.run_raw(cmd)
            if res.returncode == 0:
                print("kafka started")
                break
            time.sleep(1)

    def _setup_service_name(self) -> None:
        self.name = "/usr/bin/kafka_2.13-3.1.0"

class ServiceClustermgr1(ServiceBase):
    def _setup_service(self) -> None:
        print("starting clustermgr 1 ...")
        self.command = [f"{self.dir_manager.bin_dir}/clustermgr", "-f", f"{self.dir_manager.cfg_dir}/clustermgr1.json"]
        self.logfile = f"{self.dir_manager.log_dir}/clustermgr1-start.log"

    def _check_service(self) -> None:
        time.sleep(1)

    def _setup_service_name(self) -> None:
        self.name = "clustermgr1.json"

class ServiceClustermgr2(ServiceBase):
    def _setup_service(self) -> None:
        print("starting clustermgr 2 ...")
        self.command = [f"{self.dir_manager.bin_dir}/clustermgr", "-f", f"{self.dir_manager.cfg_dir}/clustermgr2.json"]
        self.logfile = f"{self.dir_manager.log_dir}/clustermgr2-start.log"

    def _check_service(self) -> None:
        time.sleep(1)

    def _setup_service_name(self) -> None:
        self.name = "clustermgr2.json"

class ServiceClustermgr3(ServiceBase):
    def _setup_service(self) -> None:
        print("starting clustermgr 3 ...")
        self.command = [f"{self.dir_manager.bin_dir}/clustermgr", "-f", f"{self.dir_manager.cfg_dir}/clustermgr3.json"]
        self.logfile = f"{self.dir_manager.log_dir}/clustermgr3-start.log"

    def _check_service(self) -> None:
        print("checking clustermgr ...")
        url = "http://127.0.0.1:9998/stat"
        expected_states=("StateLeader", "StateReplicate", "StateFollower")
        while True:
            result = CommandExecutor.run_http_get_json(url)
            if isinstance(result, dict):
                raft_state = result.get('raft_status', {}).get('raftState')
                if raft_state in expected_states:
                    print("clustermgr started")
                    break
            time.sleep(1)

    def _setup_service_name(self) -> None:
        self.name = "clustermgr3.json"

class ServiceBlobnode(ServiceBase):
    def _setup_service(self) -> None:
        print("starting blobnode ...")
        self._setup_disks_dir()
        self.command = [f"{self.dir_manager.bin_dir}/blobnode", "-f", f"{self.dir_manager.cfg_dir}/blobnode.json"]
        self.logfile = f"{self.dir_manager.log_dir}/blobnode-start.log"

    def _check_service(self) -> None:
        print("checking blobnode ...")
        url = "http://127.0.0.1:8899/stat"
        while True:
            result = CommandExecutor.run_http_get_json(url)
            if isinstance(result, list) and len(result) == 8:
                print("blobnode started")
                break
            time.sleep(1)

    def _setup_disks_dir(self) -> None:
        blobnode_config = ConfigFileManager.get_json_data(f"{self.dir_manager.cfg_dir}/blobnode.json")
        for disk in blobnode_config['disks']:
            disk_path = Path(disk['path'])
            disk_path.mkdir(parents=True, exist_ok=True)

    def _setup_service_name(self) -> None:
        self.name = "blobnode"

class ServiceProxy(ServiceBase):
    def _setup_service(self) -> None:
        print("starting proxy ...")
        self.command = [f"{self.dir_manager.bin_dir}/proxy", "-f", f"{self.dir_manager.cfg_dir}/proxy.json"]
        self.logfile = f"{self.dir_manager.log_dir}/proxy-start.log"

    def _check_service(self) -> None:
        print("checking proxy ...")
        url = "http://127.0.0.1:9600/volume/list?code_mode=11"
        while True:
            result = CommandExecutor.run_http_get_json(url)
            if isinstance(result, dict) and 'vids' in result and len(result['vids']) > 0:
                print("proxy started")
                break
            time.sleep(1)

    def _setup_service_name(self) -> None:
        self.name = "proxy"

class ServiceScheduler(ServiceBase):
    def _setup_service(self) -> None:
        print("starting scheduler ...")
        self.command = [f"{self.dir_manager.bin_dir}/scheduler", "-f", f"{self.dir_manager.cfg_dir}/scheduler.json"]
        self.logfile = f"{self.dir_manager.log_dir}/scheduler-start.log"

    def _check_service(self) -> None:
        print("checking scheduler ...")
        url = "http://127.0.0.1:9800/stats"
        expected_keys=("blobnode", "shard")
        while True:
            result = CommandExecutor.run_http_get_json(url)
            if isinstance(result, dict) and all(key in result for key in expected_keys):
                print("scheduler started")
                break
            time.sleep(1)

    def _setup_service_name(self) -> None:
        self.name = "scheduler"

class ServiceShardnode(ServiceBase):
    def _setup_service(self) -> None:
        print("starting shardnode ...")
        self._setup_disks_dir()
        self.command = [f"{self.dir_manager.bin_dir}/shardnode", "-f", f"{self.dir_manager.cfg_dir}/shardnode.json"]
        self.logfile = f"{self.dir_manager.log_dir}/shardnode-start.log"

    def _check_service(self) -> None:
        print("checking shardnode ...")
        url = "http://127.0.0.1:9101/blob/delete/stats"
        expected_keys=("success_per_min", "failed_per_min")
        while True:
            result = CommandExecutor.run_http_get_json(url)
            if isinstance(result, dict) and all(key in result for key in expected_keys):
                print("scheduler started")
                break
            time.sleep(1)

    def _setup_disks_dir(self) -> None:
        shardnode_config = ConfigFileManager.get_json_data(f"{self.dir_manager.cfg_dir}/shardnode.json")
        disks = shardnode_config.get("disks_config", {}).get("disks", [])
        for disk_path in disks:
            Path(disk_path).mkdir(parents=True, exist_ok=True)

    def _setup_service_name(self) -> None:
        self.name = "shardnode"

class VstartManager:
    def __init__(self):
        self.args = self._parse_args()
        self.dir_manager = DirectoryManager()
        self.service_classes = [
            ServiceConsul,
            ServiceKafka,
            ServiceClustermgr1,
            ServiceClustermgr2,
            ServiceClustermgr3,
            ServiceBlobnode,
            ServiceProxy,
            ServiceScheduler,
            ServiceShardnode,
        ]

    def _parse_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser(description='Entrypoint for blobstore, used to start all services.')
        parser.add_argument('--rmdir', action="store_true", help='Remove all relative directories.')
        parser.add_argument('--start-services', action="store_true", help='Start all services.')
        parser.add_argument('--stop-services', action="store_true", help='Stop all services.')
        return parser.parse_args()

    def run(self) -> None:
        if self.args.stop_services:
            for service in self.service_classes:
                service(self.dir_manager).stop_service()
                time.sleep(1)

        if self.args.rmdir:
            self.dir_manager.remove_directory()

        if self.args.start_services:
            self.dir_manager.setup_directory()
            for service in self.service_classes:
                service(self.dir_manager).run_service()


def main():
    if sys.version_info.major < 3:
        print(f"Error: Python 3 or higher is required, but found {sys.version}")
        sys.exit(1)

    VstartManager().run()

if __name__ == "__main__":
    main()
