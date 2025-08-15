# Examples


## File management
For many tools, you will need to manage input and output files for your tool calls. 

Files are keyed as UUIDv4 strings and are accepted as input for tool calls requiring input files. 

### Uploading a file
The following script lets you upload a file from your local directory to the Rhea MCP server. 

The script will output a UUIDv4 string representing the file string.

This script can be found in [examples/upload_file.py](https://github.com/chrisagrams/rhea/blob/main/examples/upload_file.py)

``` python
--8<-- "examples/upload_file.py"
```

1. Open a connection with the MCP server with an asynchronous context manager.
2. Call `upload_file()` with the path of your file.
3. This is the file key for your uploaded file to be used as input for tools.
4. Make sure to run as a coroutine.

Usage:
``` bash
python upload_file.py /path/to/file --url http://localhost:3001
```

### Downloading a file
The following scripts lets you download a file from the Rhea MCP server to a local directory.

This script can be found in [examples/download_file.py](https://github.com/chrisagrams/rhea/blob/main/examples/download_file.py)

```python
--8<-- "examples/download_file.py"
```

1. Open a connection with the MCP server with an asynchronous context manager. 
2. Call `download_file()` with your desired file key and output path.
3. Make sure to run as a coroutine.

Usage:
```bash
python download_file.py file_key /path/to/output/directory
```