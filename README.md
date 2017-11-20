# Python Multithread Line By Line File Worker
A small module helps to process (long time consuming operations) line by line file simultaneously

### Common use cases for using PythonMultithreadLineByLineFileWorker:
1. HTTP request for each line in the file

### Common use cases for not using PythonMultithreadLineByLineFileWorker:
1. Short time consuming operations like arithmetic opertations

### Installation

### Usage
1. import `LineByLineJob` class from the module: 
```python
from line_by_line_multithread_worker import LineByLineJob
```
2. create a worker function with signature as follows: 
```python
def worker(row):
    result = make_some_process_on_single_row(row)
    return result
```

where `make_some_process_on_single_row` is a function makes process on a single row and returns a string (if `is_csv=True` on step 3 then it should return an array of strings) to append to the result file, if no result needed just return `False` in the worker function as follows:

```python
def worker(row):
    make_some_process_on_single_row(row)
    return False
```
3. Create a job instance:
```python
job = LineByLineJob(input_file_path=PATH_TO_THE_INPUT, single_line_worker=worker, is_csv=False, number_of_threads=20, debug=True, output_file_name="MY_FILE_NAME.txt", output_directory_path=PATH_TO_OUTPUT_DIR)
```

**input_file_path:** input file with lines to process

**single_line_worker:** a reference to a function that processes a single line from the input file ('''def worker(single_line):''')

**number_of_threads:** number of threads that will simultaneously work on the lines of the source file

**output_directory_path:** full path of the output file, if not set output file would be created on the current working directory

**output_file_name:** output file name

**is_csv:** if is_csv set to true each line returned as array of csv line split by , (comma) [worker should also return proccessed array and not string]

**debug:** verbose logging and keeps temp files

4. Execute task anywhere in the code:
```python
task.start()
```

### Examples
Examples provided in the `/examples` directory
1. Spliting file into separate files
2. Process csv file
3. more examples to come...

### Todo's:
- [ ] installation out of the box
- [ ] completion handler
- [ ] HTTP request example
