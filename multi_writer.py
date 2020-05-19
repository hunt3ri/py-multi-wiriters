from concurrent.futures import ThreadPoolExecutor
from functools import partial
from multiprocessing import Manager, Pool, Queue
from random import randint
from queue import Queue
import time
import threading

def run_multi():
    customers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
                 29, 30, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26,
                 27, 28, 29, 30]
    with Manager() as manager:
        pool = Pool()
        success_queue = manager.Queue()
        failure_queue = manager.Queue()
        pool.apply_async(file_writer, (success_queue, failure_queue))
        pool.map(partial(worker, success_queue=success_queue, failure_queue=failure_queue), customers)


def worker(customer: int, success_queue: Queue, failure_queue: Queue):
    print(f"Hello {customer}")
    if customer % 2 == 0:
        print("success")
        success_queue.put(customer)
    else:
        print("Failure")
        failure_queue.put(customer)

    time.sleep(0.2)


def file_writer(success_queue: Queue, failure_queue: Queue):
    print("Starting Listener")

    with open("success.txt", "a") as success, open("failure.txt", "a") as failure:
        while True:
            print("Hello Iain")
            success.write(f"Success: {success_queue.get()}\n")
            failure.write(f"Failure: {failure_queue.get()}\n")


if __name__ == "__main__":
    start_time = time.perf_counter()
    run_multi()
    end_time = time.perf_counter()
    print(f"Total time = {end_time - start_time:0.4f}")