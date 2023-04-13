import myutil
import numpy as np
from Pyfhel import Pyfhel, PyCtxt

def create_he_object(config=None, n=1<<14, t_bits=18, sec=128):
    if config != None:
        n = 1 << config["poly_modulus_degree_bit_size"]
        t_bits = config["plain_modulus_bit_size"]
        sec = config["security_parameter"]
    HE = Pyfhel()
    bfv_params = {
        'scheme': 'BFV',
        'n': n,      
        't_bits': t_bits,
        'sec': sec,        
    }
    HE.contextGen(**bfv_params)
    HE.keyGen()
    HE.rotateKeyGen()
    HE.relinKeyGen()
    return HE

def save_public_to_bytes(HE): 
    return HE.to_bytes_context(), HE.to_bytes_public_key(), HE.to_bytes_rotate_key(), HE.to_bytes_relin_key()

def load_public_from_bytes(pub_keys_bytes):
    HE_pub = Pyfhel()
    HE_pub.from_bytes_context(pub_keys_bytes[0])
    HE_pub.from_bytes_public_key(pub_keys_bytes[1])
    HE_pub.from_bytes_rotate_key(pub_keys_bytes[2])
    HE_pub.from_bytes_relin_key(pub_keys_bytes[3])
    return HE_pub

def copy_cipher_list(ciphers):
    return [cipher.copy() for cipher in ciphers]

def sum_cipher(cipher, HE):
    n_bits = int(np.log2(HE.n))
    cipher += HE.flip(cipher, True)
    for i in range(n_bits-2, -1, -1):
        cipher += HE.rotate(cipher, 1<<i, True)
    return cipher

def apply_elementwise_mapping(mapping_cipher, mapping_offset, mapping_bit_width, x, HE): # Note that mapping_cipher can't be modified inplace
    n_bits, mapping_width = int(np.log2(HE.n)), 1 << mapping_bit_width
    # Mask out other mappings in the ciphertext
    mask = np.zeros(HE.n, dtype=np.int64)
    mask[mapping_offset:mapping_offset+mapping_width] = 1
    mapping_cipher = mapping_cipher * HE.encodeInt(mask)
    # Repeat the mapping to fill up the ciphertext
    mapping_cipher += HE.flip(mapping_cipher, True)
    for i in range(n_bits-2, mapping_bit_width-1, -1):
        mapping_cipher += HE.rotate(mapping_cipher, 1<<i, True)
    # Prepare all possible rotations
    rotated_mapping_ciphers = [mapping_cipher]
    for i in range(1, mapping_width):
        rotated_mapping_ciphers.append(HE.rotate(rotated_mapping_ciphers[-1], 1, True))
    # Construct indicator vectors in batch
    batched_ind = np.zeros((mapping_width, HE.n), dtype=np.int64)
    assert x.shape[0] <= HE.n
    for i in range(x.shape[0]):
        batched_ind[(x[i]-i)%mapping_width][i] = 1
    # Index values with correct locations
    y_cipher = HE.encryptInt(np.zeros(HE.n, dtype=np.int64))
    for i in range(mapping_width):
        if any(batched_ind[i]):
            rotated_mapping_ciphers[i] *= HE.encodeInt(batched_ind[i])
            y_cipher += rotated_mapping_ciphers[i]
    return y_cipher


if __name__ == "__main__":
    # Some tests
    test_id = 0
    if test_id == 1:
        print("Test 1: Basic Encryption and Decryption")
        HE = create_he_object()
        pub_keys_bytes = save_public_to_bytes(HE)
        HE_pub = load_public_from_bytes(pub_keys_bytes)
        arr1 = np.array([1, 2, 3], dtype=np.int64)
        arr2 = np.array([1, 1, 1], dtype=np.int64)
        ctxt1 = HE.encryptInt(arr1)
        ctxt2 = HE.encryptInt(arr2)
        _ctxt1 = PyCtxt(pyfhel=HE_pub)
        _ctxt1.from_bytes(ctxt1.to_bytes())
        _ctxt2 = PyCtxt(pyfhel=HE_pub)
        _ctxt2.from_bytes(ctxt2.to_bytes())
        _ctxt3 = _ctxt1 * _ctxt2
        ~_ctxt3
        arr3 = HE.decryptInt(_ctxt3)
        print("arr1 =", arr1, "\narr2 =", arr2, "\narr1 * arr2 =", arr3)
    elif test_id == 2:
        print("Test 2: sum_cipher")
        HE = create_he_object()
        arr = np.array([1, 2, 3, 4], dtype=np.int64)
        ctxt = HE.encryptInt(arr)
        print("arr =", arr, "\nsum_cipher =", HE.decryptInt(sum_cipher(ctxt, HE)))
    elif test_id == 3:
        print("Test 3: apply_elementwise_mapping")
        HE = create_he_object()
        # Construct mapping [1, 5, 10, 15, 20, 1, ... ,1]
        mapping = np.ones(1<<14, dtype=np.int64)
        mapping[10*(1<<8)+1:10*(1<<8)+5] = [5, 10, 15, 20]
        mapping_cipher = HE.encryptInt(mapping)
        x = np.zeros(1<<14, dtype=np.int64)
        x[1], x[3], x[5], x[7] = 1, 2, 3, 4
        x[(1<<8)], x[(1<<8)+2], x[(1<<8)+4], x[(1<<8)+6] = 1, 2, 3, 4
        y_cipher = apply_elementwise_mapping(mapping_cipher, 10*(1<<8), 8, x, HE)
        y = HE.decryptInt(y_cipher)
        print("x[0:8] =", x[0:8], "\ny[0:8] =", y[0:8])
        print("x[(1<<8):(1<<8)+8] =", x[(1<<8):(1<<8)+8], "\ny[(1<<8):(1<<8)+8] =", y[(1<<8):(1<<8)+8])

    # Test running time of HE operations
    test_time = True
    if test_time == True:
        HE = create_he_object()
        arr1 = np.arange(HE.n, dtype=np.int64)
        arr2 = np.ones(HE.n, dtype=np.int64) * 3
        ptxt1 = HE.encodeInt(arr1)
        ptxt2 = HE.encodeInt(arr2)
        ctxt1 = HE.encryptPtxt(ptxt1)
        ctxt2 = HE.encryptPtxt(ptxt2)
        myutil.report_time("HE Operation - Plaintext Add (per record)", 0)
        ctxt3 = ctxt1 + ptxt2
        myutil.report_time("HE Operation - Plaintext Add (per record)", 1, HE.n)
        myutil.report_time("HE Operation - Ciphertext Add (per record)", 0)
        ctxt3 = ctxt1 + ctxt2
        myutil.report_time("HE Operation - Ciphertext Add (per record)", 1, HE.n)
        myutil.report_time("HE Operation - Plaintext Mul (per record)", 0)
        ctxt3 = ctxt1 * ptxt2
        myutil.report_time("HE Operation - Plaintext Mul (per record)", 1, HE.n)
        myutil.report_time("HE Operation - Ciphertext Mul (per record)", 0)
        ctxt3 = ctxt1 * ctxt2
        myutil.report_time("HE Operation - Ciphertext Mul (per record)", 1, HE.n)
        myutil.report_time("HE Operation - Relinearization (per record)", 0)
        ~ctxt3
        myutil.report_time("HE Operation - Relinearization (per record)", 1, HE.n)
        myutil.report_time("HE Operation - Rotation (per record)", 0)
        ctxt3 = ctxt3 << 2
        myutil.report_time("HE Operation - Rotation (per record)", 1, HE.n)
        myutil.write_event_times()
