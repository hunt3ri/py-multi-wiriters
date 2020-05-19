from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import threading
import time
from functools import partial


def file_writer(success_queue: Queue, failure_queue: Queue, event):
    """ Use queues to write to files, we need to wait for a threading event to exit the loop"""
    with open("success.txt", "a") as success, open("failure.txt", "a") as fail:
        while not event.is_set() or not failure_queue.empty() or not success_queue.empty():
            success.write(f"Failure: {success_queue.get()}\n")
            fail.write(f"Failure: {failure_queue.get()}\n")


def worker(customer: int, success_queue: Queue, failure_queue: Queue):
    """ Sample worker """
    print(f"Hello {customer}")
    if customer % 2 == 0:
        print("success")
        success_queue.put(customer)
    else:
        print("Failure")
        failure_queue.put(customer)

    time.sleep(0.2)


def run_threaded():
    """ Run workload threaded """
    customers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30]

    event = threading.Event()
    success_queue = Queue()
    failure_queue = Queue()
    with ThreadPoolExecutor(max_workers=10) as executor:
        print("start worker")
        executor.map(partial(worker, success_queue=success_queue, failure_queue=failure_queue), customers)
        executor.submit(file_writer, success_queue, failure_queue, event)
        time.sleep(0.1)
        print("Main: about to set event")
        event.set()

    print("Ends")


if __name__ == "__main__":
    start_time = time.perf_counter()
    run_threaded()
    end_time = time.perf_counter()
    print(f"Total time = {end_time - start_time:0.4f}")

