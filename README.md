# Files transfer

The script is made to send data using LFTP to an FTP server.

The script use LFTP log file to determine the last file sent.

## Requirements

- Code should work with python >=3.7
- Only dependency is toml package

## Configuration of script

Create a `config_transfer.toml` file following example in `conf` directory

```toml
[FTP]
server = "ftp.server.fr"
user = "ftp_user"
password = "ftp_password"
port = 21
dir = "/ftp/directory"

[files]
dir_mask = "test/data"
file_mask = "T3250605_%Y%m%d_%H%M%S.nc"
```

The script will look at the options `dir_mask` and `file_mask` in the `[files]` section. You can use the format code below to indicate the format of dates and time in directory and file name

| Directive | Meaning                            |    Example     |
| --------- | ---------------------------------- | :------------: |
| `%Y`      | Year with century                  | 0001, .., 2022 |
| `%y`      | Year without century (zero-padded) |   00, .., 22   |
| `%m`      | Month (zero-padded)                |   01, .., 12   |
| `%d`      | Day (zero-padded)                  |   01, .., 31   |
| `%H`      | Hour (zero-padded)                 |   00, .., 23   |
| `%M`      | Minute (zero-padded)               |   00, .., 59   |
| `%S`      | Second (zero-padded)               |   00, .., 59   |


## Running the script

### First run

For the first run, the script needs to create the first  lftp log file and so it needs to be provided the first date to look for files.

```bash
python lftp_transfer.py conf/conf_transfer.toml test/log/test.log --since '2023-01-15'
```

### Other runs

```bash
python lftp_transfer.py conf/conf_transfer.toml test/log/test.log
```




