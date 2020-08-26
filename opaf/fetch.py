"""
Module for fetching OPA packages.

put opaf.sh in user/local/bin

Then we can do the following:

Installing OPA packages
---------------------------------
opaf install --read opa_reqs.yaml

or

opaf install --id <your artifact id> --version <your version> [--repo <your_repo>]

Uninstall OPA packages
---------------------------------
opaf uninstall --id <your artifact id> [--version <your version>]
"""
import argparse
import os
import subprocess
import sys

import ruamel.yaml
import json
import logging


def build_logger(name):
    logger = logging.getLogger(name)
    console_handler = logging.StreamHandler()
    logger.addHandler(console_handler)
    return logger


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M')

logger = build_logger('opa fetch')

yaml2Dict = ruamel.yaml.YAML()

OPA_SITE_PACKAGES = "/usr/local/lib/opa/site-packages"
DEFAULT_REPO = "generic-local"


# sample path: /artifactory/generic-local/bnsopa-maestro-policies/0.0.1/bnsopa-maestro-policies-0.0.1.tar.gz


class Artifact:

    def __init__(self, artifact_name, version):
        self.artifact_path = "%s/%s/%s-%s.tar.gz" % (artifact_name, version, artifact_name, version)

    def with_repo(self, repo):
        af_path = "artifactory/" + repo + "/" + self.artifact_path
        return "https://${ARTIFACTORY_HOST}/%s" % af_path


def install_artifacts_from_requirements_file(path_to_dependencies):
    repos, requirements = parse_requirements_file(path_to_dependencies)
    for name, version in requirements:
        opa_package_path = get_opa_package_path(name, version)
        artifact = Artifact(name, version)
        install_artifact(artifact, opa_package_path, repos)


def parse_requirements_file(path_to_dependencies):
    requirements_file_map = yaml2Dict.load(path_to_dependencies)
    repos = requirements_file_map.get('repositories')  # Okay to be null
    requirements = requirements_file_map['requirements']  # This must be present
    return repos, requirements


def install_artifact(artifact, site_package_path, repos):
    artifact_downloaded = download(artifact, repos, site_package_path)
    if artifact_downloaded:
        extract_artifact(site_package_path)
    else:
        logger.warning(
            "Could not find artifact with path %s in any of the given repositories" % artifact.artifact_path
        )


def extract_artifact(site_package_path):
    execute_command('tar xf %s --strip-components=1' % site_package_path)
    execute_command('rm site_package_path')


def download(artifact, repos, site_package_path):
    if repos is None:
        return try_downloading_from_this_repo(artifact, site_package_path, DEFAULT_REPO)
    return try_downloading_from_all_repos(artifact, site_package_path, repos)


def try_downloading_from_all_repos(artifact, repos, site_package_path):
    artifact_piped_to_site_package = False
    repo_index = 0
    while not artifact_piped_to_site_package and repo_index < len(repos):
        repo = repos[repo_index]
        artifact_piped_to_site_package = try_downloading_from_this_repo(artifact, site_package_path, repo)
        repo_index += 1
    return artifact_piped_to_site_package


def try_downloading_from_this_repo(artifact, repo, site_package_tar_dump_path):
    try:
        artifactory_url = artifact.with_repo(repo)
        return download_to_path(artifactory_url, site_package_tar_dump_path)
    except Exception as e:
        logger.error(e)
        return False


def get_opa_package_path(artifact_name, version):
    return OPA_SITE_PACKAGES + "/%s/%s" % (artifact_name, version)


def download_to_path(resource_url, path):
    command = "curl %s >> %s" % (resource_url, path)
    data = execute_command(command)
    return check_if_resource_piped_to_path(data)


def execute_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    data, err = process.communicate()
    raise_exception_if_process_exited_abnormally(err, process)
    return data


def raise_exception_if_process_exited_abnormally(err, process):
    if err:  # check stderr for messages, even if process return code is 0
        raise Exception(err)
    elif process.returncode != 0:
        raise Exception("Error during curl request!")


def check_if_resource_piped_to_path(curl_output):
    """
    :param curl_output: The output of the curl process.
    :return: true if the AF resource was successfully piped to the path
    :raise Exception if an error other than 404 occurred
    """
    decoded = curl_output.decode('utf-8')
    errors = json.loads(decoded)['errors']
    if len(errors) > 0:
        return False
    return True


def build_arg_parser(args):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     prog='opaf')
    required_group = parser.add_argument_group('Required arguments.')
    if args[0] == "install":
        if args[1] == '--read':
            required_group.add_argument('--read', help='The path to the opa package requirements file.', required=True)
        else:
            required_group.add_argument('--id', help='The artifact id.', required=True)
            required_group.add_argument('--version', help='The artifact version.', required=True)
            required_group.add_argument('--repo', help='The artifact repo.', required=False)
    elif args[0] == "uninstall":
        required_group.add_argument('--id', help='The artifact id.', required=True)
        required_group.add_argument('--version', help='The artifact version.', required=False)
    return parser


def install_package(parsed_args):
    if not parsed_args.read:
        install_artifacts_from_requirements_file(parsed_args.read)
    else:
        name, version, repo = parsed_args.id, parsed_args.version, parsed_args.repo
        opa_package_path = get_opa_package_path(name, version)
        artifact = Artifact(name, version)
        install_artifact(artifact, opa_package_path, [repo])


def uninstall_package(parsed_args):
    path = OPA_SITE_PACKAGES + '/' + parsed_args.id
    if parsed_args.version:
        path += '/' + parsed_args.version
    execute_command("rm -rf " + path)


def main(args):
    parser = build_arg_parser(args)
    parsed_args = parser.parse_args(args)
    if args[0] == 'install':
        install_package(parsed_args)
    elif args[0] == 'uninstall':
        uninstall_package(parsed_args)


if __name__ == '__main__':
    # 0th argument is the name of this file
    main(sys.argv[1:])
