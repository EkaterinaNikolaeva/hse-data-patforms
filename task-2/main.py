import subprocess
import argparse


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")
    prepare_parser = subparsers.add_parser("prepare", help="prepare for launch")
    prepare_parser.set_defaults(func=action_prepare)
    run_parser = subparsers.add_parser("hdfs", help="Run configuration HDFS cluster")
    run_parser.set_defaults(func=action_hdfs)
    yarn_parser = subparsers.add_parser("yarn", help="Run YARN configuration")
    yarn_parser.set_defaults(func=action_yarn)
    test_parser = subparsers.add_parser("test", help="Test MapReduce functionality")
    test_parser.set_defaults(func=action_test)
    clean_parser = subparsers.add_parser(
        "clean", help="clear everything related to hdfs"
    )
    clean_parser.set_defaults(func=action_clean)
    return parser.parse_args()


def action_prepare():
    subprocess.check_call(["sudo", "apt", "update"])
    subprocess.check_call(["sudo", "apt", "install", "ansible", "-y"])
    subprocess.check_call(["sudo", "apt", "install", "sshpass", "-y"])
    subprocess.check_call(["ansible", "--version"])



def action_hdfs():
    subprocess.check_call(["ansible", "all", "-m", "ping"])
    subprocess.check_call(["ansible-playbook", "run_hdfs.yml"])


def action_yarn():
    subprocess.check_call(["ansible", "all", "-m", "ping"])
    subprocess.check_call(["ansible-playbook", "run_yarn.yml"])


def action_test():
    subprocess.check_call(["ansible", "all", "-m", "ping"])
    subprocess.check_call(["ansible-playbook", "yarn_test/test_mapreduce.yml"])


def action_clean():
    subprocess.check_call(["ansible-playbook", "cleanup.yml"])


def main():
    args = parse_args()
    args.func()


if __name__ == "__main__":
    main()
