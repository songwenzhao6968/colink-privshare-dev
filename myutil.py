from colink import CoLink, byte_to_int

max_length = 4000000

def create_large_entry(cl: CoLink, key_prefix, value_bytes): # Note that it might be unnecessary in later version of colink
    # Split data into chunks of max_length
    n_chunks = 0
    for i, l in enumerate(range(0, len(value_bytes), max_length)):
        r = min(l+max_length, len(value_bytes))
        cl.create_entry(":".join([key_prefix, str(i)]), value_bytes[l:r])
        n_chunks += 1
    cl.create_entry(":".join([key_prefix, "n_chunks"]), n_chunks)

def read_large_entry(cl: CoLink, key_prefix):
    n_chunks = byte_to_int(cl.read_entry(":".join([key_prefix, "n_chunks"])))
    value_bytes = bytes()
    for i in range(n_chunks):
        value_bytes += cl.read_entry(":".join([key_prefix, str(i)]))
    return value_bytes