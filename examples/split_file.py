import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'line_by_line_multithread_worker')))

from line_by_line_multithread_worker import LineByLineJob


def worker(input_line):
    return "split -> " + input_line

a = LineByLineJob(os.path.join(os.path.dirname(os.path.abspath(__file__)), "rows.txt"), worker, number_of_threads=100, debug=True, output_file_name="moshiko.txt", output_directory_path=os.path.join(os.getcwd(), 'output'))
a.start()
