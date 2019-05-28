import argparse
from config import CONFIGS, TOPIC_NAME
from produser import Producer


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--t', type=int, default=5,
                        help='How many threads?')
    parser.add_argument('--n', type=int, default=500,
                        help='What is the number of messages per thread')
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    for i in range(1, args.t + 1):
        p = Producer(thread_name=f'Thread {i}', number_of_messages=args.n, topic_name=TOPIC_NAME)
        p.start()
