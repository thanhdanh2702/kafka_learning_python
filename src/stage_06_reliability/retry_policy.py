import time

def run_with_retry(operation, max_attempts=3):
    for attempt in range(1, max_attempts + 1):
        try:
            return operation()
        except TimeoutError:
            if attempt == max_attempts:
                raise
            delay_seconds = 2 ** (attempt - 1)
            print(f"RETRY attempt={attempt} sleep={delay_seconds}")
            time.sleep(delay_seconds)
