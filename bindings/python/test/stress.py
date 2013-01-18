import sys
import kiwi.db
import random

def get_keys(amount):
    keys = range(amount)
    random.shuffle(keys)
    return map(lambda x: "key-%d" % x, keys)

def main(action, count):
    db = kiwi.db.DB('/tmp')
    keys = get_keys(count)

    if action == 'read':
        for key in keys:
            db.get(key)
    if action == 'write':
        for key in keys:
            db.add(key, "A" * 80)

    db.close()

if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]))
