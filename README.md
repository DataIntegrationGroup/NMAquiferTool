# NMAquiferTool


# Example usage
## Upload a Water Levels xlsx file

```shell
nmat upload waterlevels --file water_levels.xlsx
```

by default the application will run in "dry-run" mode, i.e. data will not be added to database. 
To add data to database, use the `--commit` flag:

```shell
nmat upload waterlevels --commit --file water_levels.xlsx
```

You can add verbose output with the `--verbose` flag:

```shell
nmat upload waterlevels --commit --verbose --file water_levels.xlsx
```