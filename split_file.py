from multithread_worker import LineByLineJob


def worker(input_line):
    return "split -> " + input_line

a = LineByLineJob(input_file_path="rows.txt", single_line_worker=worker, debug=True)
a.start()
