import math
import multiprocessing
import time

import numpy as np


def is_sorted(arr):
    """
    Checks if a list is sorted in ascending order.

    Parameters
    ----------
    arr (list): List of elements

    Returns
    -------
    True if the list is sorted in ascending order, False otherwise.
    """
    n = len(arr)
    for i in range(1, n):
        try:
            if arr[i] < arr[i - 1]:
                return False
        except TypeError:
            if isinstance(arr[i], float) and isinstance(arr[i - 1], str):
                return False
    return True


def check_input(min, max, processes, chunks):
    """
    Validates user input for the parallel bubble sort function.

    Parameters
    ----------
    min (float): Minimum value in the list to be sorted (must be non-negative).
    max (float): Maximum value in the list to be sorted.
    processes (int): Number of processes to use for sorting (must be at least 1).
    chunks (int): Number of chunks to use for sorting (must be at least 1).

    Returns
    -------
    bool: True if all inputs are valid, False otherwise.
    """
    if processes < 1:
        print("Number of processes should be greater than zero")
        return False
    if chunks < 1:
        print("Number of chunks should be greater than zero")
        return False
    if abs(max - min) < chunks:
        print("Warning: For better performance, the number of chunks should be less than range of elements")
    if chunks < processes:
        print("Warning: For better performance, the number of processes should be less than number of chunks")
    return True


def bubblesort(arr):
    """
    Sorts a list of elements in ascending order using the bubble sort algorithm.

    Parameters
    ----------
    arr (list): The list to be sorted.

    Returns
    -------
    arr: The sorted list.
    """
    n = len(arr)
    for i in range(n):
        swap = False
        for j in range(0, n - i - 1):
            if arr[j] > arr[j + 1]:
                arr[j], arr[j + 1] = arr[j + 1], arr[j]
                swap = True
        if not swap:
            break
    return arr


def divide_chunks(arr, amount_chunks):
    """
    Divides a list into a specified number of chunks with balanced distribution.

    Parameters
    ----------
    arr (list): The list to be divided.
    amount_chunks (int): The desired number of chunks.

    Returns
    -------
    chunks: A list of sublists representing the chunks.
    """
    strings = []
    if any(isinstance(element, str) for element in arr):
        strings = [arr.pop(i) for i in reversed(range(len(arr))) if isinstance(arr[i], str)]
    # We use ceil and floor to make range wider; also because random.uniform will never generate elements on the range boundaries
    biggest_item = math.ceil(max(arr))
    smallest_item = math.floor(min(arr))
    split_factor = (biggest_item - smallest_item) // amount_chunks
    chunks = [[element for element in arr if
               smallest_item + split_factor * i <= element < smallest_item + split_factor * (i + 1)]
              for i in range(amount_chunks)]
    # Append elements greater than or equal to the last split factor to the last chunk
    chunks[-1].extend([element for element in arr if element >= smallest_item + split_factor * amount_chunks])
    if strings:
        chunks.append(strings)
    return chunks


def parallel_bubble_sort(arr, processes, chunks):
    """
    Sorts a list of elements in ascending order using parallel bubble sort.

    Parameters
    ----------
    arr (list): The list to be sorted.
    processes (int): The number of processes to use for parallel sorting.
    chunks (int): The desired number of chunks to divide the list into.

    Returns
    -------
    sorted_arr: The sorted list.
    """
    chunks = divide_chunks(arr, chunks)
    with multiprocessing.Pool(processes=processes) as pool:
        sorted_chunks = pool.map(bubblesort, chunks)
    sorted_arr = sum(sorted_chunks, [])
    return sorted_arr


def sequential_bubble_sort(arr, processes):
    """
    Sorts a list of elements in ascending order using sequential bubble sort.

    Parameters
    ----------
    arr (list): The list to be sorted.
    processes (int): The number of processes (ignored for sequential sorting).

    Returns
    -------
    sorted_arr: The sorted list.
    """
    chunks = divide_chunks(arr, processes)
    for chunk in chunks:
        bubblesort(chunk)
    sorted_arr = sum(chunks, [])
    return sorted_arr


def sort_test_list():
    test_lst = [10, 5.5, 'car', 'banana', 3.7, 'cherry', 'plane', 6.2, 'desk', 2.3]
    print("Test array:", test_lst)
    sorted_lst = parallel_bubble_sort(test_lst.copy(), 2, 2)
    print("Array is not sorted!") if not is_sorted(sorted_lst) else None
    print("Parallel bubble sort, resulting list: ", sorted_lst)
    sorted_lst = sequential_bubble_sort(test_lst, 2)
    print("Array is not sorted!") if not is_sorted(sorted_lst) else None
    print("Sequential bubble sort, resulting list: ", sorted_lst)


if __name__ == "__main__":
    num_processes = 9
    num_chunks = 500
    max_elem = 500
    min_elem = -500
    num_elements = 50000
    lst_num_processes = [4, 9, 16, 25]
    lst_num_chunks = [50, 100, 150, 200, 250, 500]
    # sort_test_list()
    for num_processes in lst_num_processes:
        for num_chunks in lst_num_chunks:
            for _ in range(30):
                if check_input(min_elem, max_elem, num_processes, num_chunks):
                    lst = np.random.uniform(min_elem, max_elem, num_elements)
                    print(
                        f"Sorting parameters: random values range - [{min_elem}; {max_elem}], array length - {num_elements}, "
                        f"number of processes - {num_processes}, amount of chunks - {num_chunks}")

                    start_time = time.time()
                    sorted_lst = parallel_bubble_sort(lst.copy(), num_processes if num_processes <= 60 else 60,
                                                      num_chunks)  # Windows limitation
                    end_time = time.time() - start_time
                    print(f"--- Sorted in {end_time:.3f} seconds with parallel algorithm ---")
                    print("Array is not sorted!") if not is_sorted(sorted_lst) else None

                    start_time = time.time()
                    sorted_lst = sequential_bubble_sort(lst.copy(), num_chunks)
                    end_time = time.time() - start_time
                    print(f"--- Sorted in {end_time:.3f} with sequential algorithm ---")
                    print("Array is not sorted!") if not is_sorted(sorted_lst) else None
                else:
                    print("Please check input")
