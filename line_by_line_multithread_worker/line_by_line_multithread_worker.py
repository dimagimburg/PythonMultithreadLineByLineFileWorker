import csv
import os
import random
import string
import fileinput
import shutil
import ast
from threading import Thread
from Queue import Queue


class LineByLineJob:

    def __init__(self, input_file_path, single_line_worker, number_of_threads=10, output_directory_path=None, output_file_name="output", is_csv=False, debug=False):
        """
        :param input_file_path: input file with lines to process
        :param single_line_worker: a reference to a function that processes a single line from the input file ('''def worker(single_line):''')
        :param number_of_threads: number of threads that will simultaneously work on the lines of the source file
        :param output_directory_path: full path of the output file, if not set output file would be created on the current working directory
        :param output_file_name: output file name
        :param is_csv: if is_csv set to true each line returned as array of csv line split by , (comma) [worker should also return proccessed array and not string]
        :param debug: verbose logging and keeps temp files
        """

        self.number_of_threads = number_of_threads
        self.input_file_path = input_file_path
        self.single_line_worker = single_line_worker
        self.is_csv = is_csv
        self.output_file_name = output_file_name
        self.debug = debug
        self.finished = False
        self.input_queue = Queue(self.number_of_threads)
        self.job_id = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(16)) # https://stackoverflow.com/a/30779367/2698072 (thanks :D)
        self.job_directory_full_path = os.path.join(os.getcwd(), "{}_temp".format(self.job_id))

        if output_directory_path:
            self.output_file_path = output_directory_path
        else:
            self.output_file_path = os.path.join(os.getcwd(), "{}_output".format(self.job_id))

    def start(self):
        self._prepare_temp_directory()
        self._create_and_run_threads()
        self._put_input_into_queue()
        return

    def _prepare_temp_directory(self):
        if not os.path.exists(self.job_directory_full_path):
            os.makedirs(self.job_directory_full_path)

    def _create_and_run_threads(self):
        for i in range(self.number_of_threads):

            if self.is_csv:
                t = CSVLineByLineJobThread(self.input_queue, self.single_line_worker, self.job_directory_full_path, debug=self.debug)
            else:
                t = LineByLineJobThread(self.input_queue, self.single_line_worker, self.job_directory_full_path, debug=self.debug)

            t.daemon = True
            t.start()

    def _put_input_into_queue(self):
        with open(self.input_file_path, 'rb') as input_file:
            if self.is_csv:
                input_file = csv.reader(input_file)
            for index, line in enumerate(input_file):
                if self.is_csv:
                    self.input_queue.put(line)
                else:
                    line = line.strip()
                    self.input_queue.put(line + "\n")

                if self.debug:
                    print(
                    "[DEUBG :: Line added to queue] - [line_number={line_number}] [line={line_text}]".format(**{
                        'line_number': index,
                        'line_text': str(line)
                    }))

            # block until all tasks done
            self.input_queue.join()
            self._finished()

    def _finished(self):
        print("Lines are finished processing")

        # create a single output file
        if not os.path.exists(self.output_file_path):
            os.makedirs(self.output_file_path)

        filelist = map(lambda filename: os.path.join(self.job_directory_full_path, filename), os.listdir(self.job_directory_full_path))

        with open(os.path.join(self.output_file_path, self.output_file_name), 'w') as file:
            input_lines = fileinput.input(filelist)
            file.writelines(input_lines)

        # if not debug, clean the not needed files
        if not self.debug:
            shutil.rmtree(self.job_directory_full_path)

        print("Job is done.")


class LineByLineJobThread(Thread):
    """
        This class is a thread the invokes the process on input lines located on the input queue
        The line worker is invoked with one string pulled from the input queue and the processed result from the worker should be string
    """
    # TODO: check that the input is of string instance
    def __init__(self, input_queue, line_worker, path, debug=False):
        Thread.__init__(self)
        self.line_worker = line_worker
        self.input_queue = input_queue
        self.debug = debug
        self.path = path
        self.finished = False

    def run(self):
        while not self.finished:
            # time.sleep(1)
            input_line = self.input_queue.get()
            proccessed = self.line_worker(input_line)
            if bool(proccessed):
                self.append_to_thread_file(proccessed)
            self.input_queue.task_done()
        return

    def append_to_thread_file(self, preccessed_input):
        thread_id = self.ident
        with open(os.path.join(self.path, str(thread_id) + ".mlw"), "a") as myfile:
            myfile.write(preccessed_input)


class CSVLineByLineJobThread(LineByLineJobThread):
    """
        This class is a thread the invokes the process on input lines located on the input queue
        The line worker is invoked with one array of strings pulled from the input queue and the processed result from the worker should be array of strings
    """

    # TODO: check that the input is of array of string instance
    def __init__(self, input_queue, line_worker, path, debug=False):
        LineByLineJobThread.__init__(self, input_queue, line_worker, path, debug)

    def append_to_thread_file(self, preccessed_input):
        thread_id = self.ident
        with open(os.path.join(self.path, str(thread_id) + ".csv"), "ab") as myfile:
            writer = csv.writer(myfile)
            writer.writerow(preccessed_input)
