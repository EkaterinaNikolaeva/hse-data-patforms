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
    hive_parser = subparsers.add_parser("hive", help="Run Hive configuration")
    hive_parser.set_defaults(func=action_hive)
    spark_parser = subparsers.add_parser("spark", help="Run Spark configuration")
    spark_parser.set_defaults(func=action_spark)
    yarn_test_parser = subparsers.add_parser("yarn-test", help="Test MapReduce functionality")
    yarn_test_parser.set_defaults(func=action_yarn_test)
    hive_test_parser = subparsers.add_parser("hive-test", help="Test Hive functionality")
    hive_test_parser.set_defaults(func=action_hive_test)
    clean_parser = subparsers.add_parser(
        "clean", help="clear everything related to hdfs"
    )
    clean_parser.add_argument(
        "--keep-archives", 
        action="store_true", 
        help="Keep downloaded archives (Hadoop, Hive) after cleanup"
    )
    clean_parser.set_defaults(func=action_clean)
    return parser.parse_args()


def action_prepare():
    subprocess.check_call(["sudo", "apt", "update"])
    subprocess.check_call(["sudo", "apt", "install", "ansible", "-y"])
    subprocess.check_call(["sudo", "apt", "install", "sshpass", "-y"])
    subprocess.check_call(["sudo", "apt", "install", "unzip", "-y"])
    subprocess.check_call(["ansible", "--version"])



def action_hdfs():
    subprocess.check_call(["ansible", "all", "-m", "ping"])
    subprocess.check_call(["ansible-playbook", "run_hdfs.yml"])


def action_yarn():
    subprocess.check_call(["ansible", "all", "-m", "ping"])
    subprocess.check_call(["ansible-playbook", "run_yarn.yml"])


def action_hive():
    subprocess.check_call(["ansible", "all", "-m", "ping"])
    subprocess.check_call(["ansible-playbook", "run_hive.yml"])


def action_spark():
    subprocess.check_call(["ansible", "all", "-m", "ping"])
    subprocess.check_call(["ansible-playbook", "run_spark.yml"])


def action_yarn_test():
    subprocess.check_call(["ansible", "all", "-m", "ping"])
    subprocess.check_call(["ansible-playbook", "yarn_test/test_mapreduce.yml", "-i", "inventory.ini"])


def action_hive_test():
    subprocess.check_call(["ansible", "all", "-m", "ping"])
    subprocess.check_call(["ansible-playbook", "hive_test/test_beeline.yml", "-i", "inventory.ini"])


def action_clean():
    cmd = ["ansible-playbook", "cleanup.yml"]
    
    args = parse_args()
    
    if hasattr(args, 'keep_archives') and args.keep_archives:
        cmd.extend(["-e", "keep_archives=true"])
    
    subprocess.check_call(cmd)


def main():
    args = parse_args()
    args.func()


if __name__ == "__main__":
    main()
