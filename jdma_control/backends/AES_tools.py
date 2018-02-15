#!/usr/bin/env python

from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
from Crypto.Hash import SHA1
import base64

def AES_create_key(filepath=None):
    """Create a key for encrypting using AES"""
    # create a seed from random numbers
    key_seed = get_random_bytes(128)
    # hash the random seed
    hash_object = SHA1.new()
    hash_object.update(key_seed)
    # take the first 32 bytes of the seed to create an AES 256 key
    key = hash_object.hexdigest()[0:32]
    # write the key out if the filepath is not None
    if not filepath is None:
        fh = open(filepath, 'w')
        fh.write(key)
        fh.close()
    # return the key as bytes (required by Cryto library)
    return key.encode("utf-8")


def AES_read_key(filepath):
    """Read in the key to use for encryption / decryption"""
    fh = open(filepath, 'r')
    # read and encode to bytes
    key = fh.read().encode("utf-8")[0:32]
    fh.close()
    return key


def AES_encrypt(key, plaintext):
    """Encrypt a plaintext string, given a key generated as above"""
    # create the cipher
    cipher = AES.new(key, AES.MODE_EAX)
    # create the ciphertext from the plaintext and the tag for checking
    ciphertext, tag = cipher.encrypt_and_digest(plaintext.encode("utf-8"))
    # concat all the encrypted text, nonce and tag to the cipherstring, whihc is base64 encoded
    cipherstring = (base64.b64encode(cipher.nonce)) + b"$" + base64.b64encode(tag) + b"$" + (base64.b64encode(ciphertext)) + b"$"
    return cipherstring.decode("utf-8")


def AES_decrypt(key, cipherstring):
    """Decrypt the cipherstring created above"""
    # split the string back into bytes
    split_cipher = cipherstring.encode("utf-8").split(b"$")
    nonce = base64.b64decode(split_cipher[0])
    tag = base64.b64decode(split_cipher[1])
    ciphertext = base64.b64decode(split_cipher[2])
    # create the cipher using the key and nonce info.  The key is kept privately but the nonce is part of the cipherstring
    cipher = AES.new(key, AES.MODE_EAX, nonce)
    plaintext = cipher.decrypt_and_verify(ciphertext, tag).decode("utf-8")
    return plaintext


def AES_encrypt_dict(key, plaintext_dict):
    """Encrypt the values in a dictionary (which have to be strings), without affecting the keys"""
    encrypted_dict = {}
    cipher = AES.new(key, AES.MODE_EAX)
    for k in plaintext_dict:
        encrypted_dict[k] = AES_encrypt(key, plaintext_dict[k])
    return encrypted_dict


def AES_decrypt_dict(key, encrypted_dict):
    """Decrypt the values in a dictionary, without affecting the keys"""
    plaintext_dict = {}
    for k in encrypted_dict:
        plaintext_dict[k] = AES_decrypt(key, encrypted_dict[k])
    return plaintext_dict


if __name__ == "__main__":
    key = AES_create_key()
    encrypted = AES_encrypt(key, "Hello! Blah Blah!")
    decrypted = AES_decrypt(key, encrypted)
    print(encrypted)
    print(decrypted)
