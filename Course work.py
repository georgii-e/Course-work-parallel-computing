import multiprocessing
import random
import time


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
        if arr[i] < arr[i - 1]:
            return False
    return True


def check_input(min, max, processes):
    """
    Validates user input for the parallel bubble sort function.

    Parameters
    ----------
    min (int): Minimum value in the list to be sorted (must be non-negative).
    max (int): Maximum value in the list to be sorted.
    processes (int): Number of processes to use for parallel sorting (must be at least 1).

    Returns
    -------
    bool: True if all inputs are valid, False otherwise.
    """
    if min < 0:
        print("Minimum element should be 0 or greater")
        return False
    if processes < 1:
        print("Number of processes should be greater than zero")
        return False
    if max < processes:
        print("Warning: For better performance, the number of processes should be less than the maximum element")
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
    biggest_item = max(arr)
    split_factor = biggest_item // amount_chunks
    chunks = [[element for element in arr if split_factor * i <= element < split_factor * (i + 1)]
              for i in range(amount_chunks)]
    chunks[-1].extend([element for element in arr if element >= split_factor * amount_chunks])
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


if __name__ == "__main__":
    lst_num_processes = [4, 9, 16, 25]
    lst_num_chunks = [50, 100, 150, 200, 250, 500]
    num_elements = 100000
    max_elem = 1000
    min_elem = 0
    for num_processes in lst_num_processes:
        for num_chunks in lst_num_chunks:
            for _ in range(10):
                if check_input(min_elem, max_elem, num_processes):
                    print(f"Sorting parameters: random values range - [{min_elem};{max_elem}], "
                          f"array length - {num_elements}, number of processes - {num_processes}, amount of chunks - {num_chunks}")

                    lst = [(random.randint(min_elem, max_elem)) for i in range(num_elements)]
                    start_time = time.time()
                    lst = parallel_bubble_sort(lst, num_processes if num_processes <= 60 else 60, num_chunks)  # Windows limitation
                    end_time = time.time() - start_time
                    print(f"--- Sorted in {end_time:.3f} seconds with parallel algorithm ---")
                    print("Array is not sorted!") if not is_sorted(lst) else None

                    lst = [(random.randint(min_elem, max_elem)) for i in range(num_elements)]
                    start_time = time.time()
                    lst = sequential_bubble_sort(lst, num_chunks)
                    end_time = time.time() - start_time
                    print(f"--- Sorted in {end_time:.3f} with sequential algorithm ---")
                    print("Array is not sorted!") if not is_sorted(lst) else None
                else:
                    print("Please check input")
