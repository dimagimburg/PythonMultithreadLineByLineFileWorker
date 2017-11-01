from threading import Thread
from Queue import Queue


class LineByLineJob:
    NUM_THREADS = 20
    input_queue = Queue()

    def __init__(self, input_file_path, single_line_worker, debug=False):
        self.single_line_worker = single_line_worker
        self.input_file_path = input_file_path
        self.debug = debug
        self.finished = False

    def start(self):
        self._create_and_run_threads()
        self._put_input_into_queue()
        return

    def _create_and_run_threads(self):
        for i in range(self.NUM_THREADS):
            t = LineByLineJobThread(self.input_queue, self.single_line_worker, debug=self.debug)
            t.daemon = True
            t.start()

    def _put_input_into_queue(self):
        with open(self.input_file_path, 'rb') as input_file:
            for index, line in enumerate(input_file):
                line = line.strip()
                self.input_queue.put(line + "\n")
                if self.debug:
                    print(
                    "[DEUBG ::Line added to queue] - [line_number={line_number}] [line={line_text}]".format(**{
                        'line_number': index,
                        'line_text': line
                    }))

            # block until all tasks done
            self.input_queue.join()
            self._finished()

    def _finished(self):
        print("Lines are finished processing")


class LineByLineJobThread(Thread):

    def __init__(self, input_queue, line_worker, debug=False):
        Thread.__init__(self)
        self.line_worker = line_worker
        self.input_queue = input_queue
        self.debug = debug
        self.finished = False

    def run(self):
        while not self.finished:
            # time.sleep(1)
            input_line = self.input_queue.get()
            proccessed = self.line_worker(input_line)
            self.append_to_thread_file(proccessed)
            self.input_queue.task_done()
        return

    def append_to_thread_file(self, preccessed_input):
        thread_id = self.ident
        with open(str(thread_id) + ".txt", "a") as myfile:
            myfile.write(preccessed_input)
