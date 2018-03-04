# conding: utf-8
from subprocess import check_output


def slack(channel, message):
    cmd = [
        'docker',
        'run',
        '--rm',
        'slack-notifier:latest',
        channel,
        message,
    ]

    check_output(cmd)
