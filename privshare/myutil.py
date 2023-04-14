import hashlib
import time

def int_to_uint(x):
    return x + (1 << 64)
  
def str_to_uint(str):
    # SHA-256 hashing, then use xor to reduce to 32 bits
    result = hashlib.sha256(str.encode()).hexdigest()
    ret = 0
    for i in range(0, 64, 8):
        ret ^= int(result[i:i+8], 16)
    return ret

timestamps = {}
event_times = []
def report_time(event_name, is_end, per_n=1, output_event_time=False):
    timestamps[(event_name, is_end)] = time.process_time_ns()
    if is_end:
        event_time = (timestamps[(event_name, 1)] - timestamps[(event_name, 0)])/1e3/per_n
        event_times.append((event_name, event_time))
        if output_event_time:
            print("{0}: {1:.2f}µs".format(event_name, event_time))       

def write_event_times():
    with open("running_time.txt", "w") as f:
        for event_name, event_time in event_times:
            f.write("{0}: {1:.2f}µs\n".format(event_name, event_time))   
            
if __name__ == "__main__":
    # Test
    print(str_to_uint("sdahjksdsahjkddsadhjksa"))