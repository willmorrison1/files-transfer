#!/usr/bin/env python
"""Script to send data using lftp."""
import datetime as dt
import glob
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path
from shutil import which
from typing import Dict, List

import click
import toml


# parameters
ONE_DAY = dt.timedelta(days=1)


LFTP_OPTIONS = (
    "cache flush;set net:timeout 10s;set net:max-retries 2;set net:idle 15s;debug 3"
)


def check_lftp():
    """
    Check if lftp is installed.

    Returns
    -------
    Path or None
        The path to LFTP or None if not installed.

    """
    lftp_exe = which("lftp")
    if lftp_exe is not None:
        lftp_exe = Path(lftp_exe)

    return lftp_exe


def read_config(config_file: Path) -> Dict:
    """
    Read configuration file.

    Parameters
    ----------
    config_file : Path
        Path to configuration file.

    Returns
    -------
    configparser.ConfigParser
        Configuration.

    """
    with open(config_file, "r") as fid:
        config = toml.load(fid)

    return config


def find_last_date_in_log(log_file: Path, file_mask: str) -> dt.datetime:
    """
    Find last date in log file.

    Parameters
    ----------
    log_file : Path
        Path to log file.
    file_mask : str
        File mask of the file to search for (e.g. "pref_%Y%m%d")

    Returns
    -------
    datetime.datetime
        Last date in log file.

    """
    # read file
    with open(log_file, "r") as fid:
        lines = fid.readlines()

    # keep line with get
    lines = [line for line in lines if line.startswith("get")]

    # extract all filenames
    # get -O ftp://login:password@ft.server.uk/lidar file:/07151_A202301300735_SIRTA_CL31.dat # NOQA
    files = [line.split()[-1].split("/")[-1] for line in lines]

    # get all dates
    list_dates = [dt.datetime.strptime(file, file_mask) for file in files]

    if not list_dates:
        return None

    return max(list_dates)


def create_lftp_command(
    lftp: Path, conf: Dict, data_dir: Path, log_file: Path
) -> List[str]:
    """
    Create lftp command to send data to FTP server.

    Parameters
    ----------
    lftp : Path
        Path to LFTP executable.
    conf : configparser.ConfigParser
        Configuration of FTP server.
    data_dir : Path
        The directory where data are located.
    log_file : Path
        The path to lftp log file.


    Returns
    -------
    List[str]
        The list of argument of the LFTP command.

    """
    lftp_mirror_opt = f"--log={str(log_file)} -R -p -L -v"
    lftp_command = [
        str(lftp),
        "-u",
        f"{conf['FTP']['user']},{conf['FTP']['password']}",
        f"{conf['FTP']['server']}",
        "-e",
        f"{LFTP_OPTIONS}; mirror {lftp_mirror_opt} {str(data_dir)} {conf['FTP']['dir']}; bye",  # noqa
    ]

    return lftp_command


@click.command()
@click.argument(
    "config-file",
    type=click.Path(
        exists=True,
        dir_okay=False,
        file_okay=True,
        readable=True,
        resolve_path=True,
        path_type=Path,
    ),
)
@click.argument(
    "log-file",
    type=click.Path(
        dir_okay=False,
        file_okay=True,
        writable=True,
        readable=True,
        resolve_path=True,
        path_type=Path,
    ),
)
@click.option(
    "--since",
    type=click.DateTime(formats=["%Y-%m-%d", "%Y%m%d", "%Y-%m-%dT%H:%M:%S"]),
    help="First date to send (required for first run)",
    default=None,
)
def main(config_file: Path, log_file: Path, since: dt.datetime):
    """Send data using lftp."""
    # checks before running
    # ------------------------------------------------------------------------
    # check if first creation of log file
    if not log_file.exists() and since is None:
        click.echo("ERROR: for new log-file, --since option is required.")
        sys.exit(1)

    # check if lftp is installed
    lftp_exe = check_lftp()
    if lftp_exe is None:
        click.echo("ERROR: lftp is not available. Please install it.")
        sys.exit(1)

    # prepare environment
    # ------------------------------------------------------------------------
    # create working dir
    work_dir = Path(tempfile.mkdtemp())

    # read config file
    conf = read_config(config_file)

    # get date to search for
    if since is None:
        # find last date in log file
        min_date = find_last_date_in_log(log_file, conf["files"]["file_mask"])
        click.echo(f"Date of Last sent file: {min_date}")

        if min_date is None:
            click.echo("no new file to send.")
            sys.exit(0)

    else:
        min_date = since

    now = dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + ONE_DAY

    # check dates
    if min_date >= now:
        click.echo("ERROR: since should be lower than now.")
        sys.exit(1)

    list_dates = [min_date + i * ONE_DAY for i in range((now - min_date).days)]

    # search for files corresponding to the pattern
    # ------------------------------------------------------------------------
    dir_mask = conf["files"]["dir_mask"]
    file_mask = conf["files"]["file_mask"]

    files_to_send = []
    for date in list_dates:

        dir_date_mask = re.sub(r"\%H|%M|\%S", "??", dir_mask)
        dir_date_mask = date.strftime(dir_date_mask)

        file_date_mask = re.sub(r"\%H|%M|\%S", "??", file_mask)
        file_date_mask = date.strftime(file_date_mask)

        files = sorted(glob.glob(os.path.join(dir_date_mask, file_date_mask)))

        # keep only file geater that min_date
        for file in files:
            file_dt = dt.datetime.strptime(os.path.basename(file), file_mask)
            if file_dt > min_date:
                files_to_send.append(file)

    if not files_to_send:
        click.echo("no new file to send.")
        sys.exit(0)

    # create symlink in tmp dir
    for file in files_to_send:
        os.symlink(file, os.path.join(work_dir, os.path.basename(file)))

    # create lftp command
    lftp_cmd = create_lftp_command(lftp_exe, conf, work_dir, log_file)
    cmd_ret = subprocess.run(lftp_cmd, capture_output=True, encoding="utf-8")

    click.echo(f"lftp command: {cmd_ret.args}")
    click.echo(f"lftp return code: {cmd_ret.returncode}")
    click.echo(f"lftp stdout: {cmd_ret.stdout}")
    click.echo(f"lftp stderr: {cmd_ret.stderr}")

    sys.exit(0)


if __name__ == "__main__":
    main()
