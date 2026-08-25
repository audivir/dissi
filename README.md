# dissi

Wraps a program or Python script and logs its execution to a Discord webhook.

## Prerequisites

- Python 3.10 or newer
- A Discord webhook URL

## Installation

```shell
pip install git+https://github.com/audivir/dissi
```

## Usage

```shell
dissi --webhook <discord-webhook-url> <program> [args...]
```

The webhook URL can also be set via the `DISCORD_WEBHOOK` environment variable, or in a `.env`
file in the working directory.

Options:

- `--force-exec`: run program as subprocess instead of trying to import it as Python module first.
- `--force-py`: run program as Python module instead of falling back to subprocess execution.
- `--grep <pattern>`: forward stdout/stderr lines matching pattern to Discord, may be given
  multiple times.
- `--title <title>`: title for the Discord message, requires `--grep`.
- `--only-log`: forward only matched lines to stdout/stderr instead of all output, requires
  `--grep`.

On exit, dissi sends the exit code, and on failure a traceback, to the configured webhook.

## Acknowledgments

dissi is a port of [DiscordWebhookLogging](https://github.com/matobodo/DiscordWebhookLogging) by
matobodo, licensed under the MIT License. Portions of the message-sending logic in
`dissi/main.py` are adapted from [discord-logger](https://github.com/2press/discord-logger) by
2press, also licensed under the MIT License. See `NOTICE` for details.

## License

MIT, see `LICENSE`.
