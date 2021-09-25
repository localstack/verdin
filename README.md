Verdin
======

<p>
  <a href="https://pypi.org/project/verdin/"><img alt="PyPI Version" src="https://img.shields.io/pypi/v/verdin?color=blue"></a>
  <a href="https://img.shields.io/pypi/l/verdin.svg"><img alt="PyPI License" src="https://img.shields.io/pypi/l/verdin.svg"></a>
  <a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
</p>

Verdin is a [tiny bird](https://en.wikipedia.org/wiki/Verdin), and also a Python SDK for [Tinybird](https://tinybird.co)
.

Install
-------

    pip install verdin

Usage
-----

### Query a Pipe

```python
# the tinybird module exposes all important tinybird concepts
from verdin import tinybird

client = tinybird.Client("p.mytoken")
pipe = client.pipe("my_pipe")

# query the pipe using dynamic parameters
response: tinybird.PipeJsonResponse = pipe.query({"key": "val"})

# print records returned from the pipe
print(response.data)
```

### Append to a DataSource

```python
from verdin import tinybird

client = tinybird.Client("p.mytoken")

# will access my_datasource__v0
datasource = client.datasource("my_datasource", version=0)

# query the pipe using dynamic parameters
datasource.append([
    ("col1-row1", "col2-row1"),
    ("col1-row2", "col2-row2"),
])
```

### Queue and batch records into a DataSource

Verdin provides a way to queue and batch data continuously:

```python
from queue import Queue
from threading import Thread

from verdin import tinybird
from verdin.worker import QueuingDatasourceAppender

client = tinybird.Client("p.mytoken")

records = Queue()

appender = QueuingDatasourceAppender(records, client.datasource("my_datasource"))
Thread(target=appender.run).start()

# appender will regularly read batches of data from the queue and append them
# to the datasource. the appender respects rate limiting.

records.put(("col1-row1", "col2-row1"))
records.put(("col1-row2", "col2-row2"))
```

Develop
-------

Create the virtual environment, install dependencies, and run tests

    make venv
    make test

Run the code formatter

    make format

Upload the pypi package using twine

    make upload
