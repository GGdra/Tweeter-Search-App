import time
from collections import OrderedDict
import pickle
import os

class LRUCacheWithTTL:
    def __init__(self, capacity, ttl=3600, checkpoint_file='cache_checkpoint.pkl'):
        self.cache = OrderedDict()
        self.capacity = capacity
        self.ttl = ttl
        self.checkpoint_file = checkpoint_file
        self.load_checkpoint()

    def get(self, key):
        if key in self.cache and not self.is_entry_stale(key):
            self.cache.move_to_end(key)
            return self.cache[key][0]
        else:
            return None

    def put(self, key, value):
        if key in self.cache:
            del self.cache[key]
        elif len(self.cache) >= self.capacity:
            self.evict_lru()
        self.cache[key] = (value, time.time())

    def evict_lru(self):
        key, _ = self.cache.popitem(last=False)
        print(f"Evicted: {key}")

    def is_entry_stale(self, key):
        if key in self.cache:
            entry_time = self.cache[key][1]
            return (time.time() - entry_time) > self.ttl
        return True  # Treat missing keys as stale

    def purge_stale_entries(self):
        keys_to_delete = [key for key, (_, timestamp) in self.cache.items() if (time.time() - timestamp) > self.ttl]
        for key in keys_to_delete:
            del self.cache[key]
            print(f"Purged: {key}")

    def checkpoint(self):
        with open(self.checkpoint_file, 'wb') as f:
            pickle.dump(self.cache, f)
        print("Checkpoint created.")

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, 'rb') as f:
                loaded_cache = pickle.load(f)
                self.cache = OrderedDict()
                for k, v in loaded_cache.items():
                    if not self.is_entry_stale(k):
                        self.cache[k] = v
            print("Checkpoint loaded.")

    def periodic_checkpoint(self, interval):
        while True:
            self.purge_stale_entries()  # Purges stale entries before creating a checkpoint
            self.checkpoint()
            time.sleep(interval)  # Interval in seconds


if __name__ == '__main__':

    # Example usage:
    cache = LRUCacheWithTTL(capacity=5, ttl=3600)
    cache.put("some_key", "some_value")
    time.sleep(2)  # Simulate passage of time
    value = cache.get("some_key")  # Should return 'some_value' if within TTL
    print(value)
    cache.periodic_checkpoint(600)  # Set the checkpoint interval as needed


