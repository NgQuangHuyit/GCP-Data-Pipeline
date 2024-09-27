import argparse
import os
import pandas as pd
import threading
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - [%(threadName)s] - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def transform(input_path, output_path):
    logger.info(f"Transforming {input_path}...")
    df = pd.read_json(input_path, lines=True)
    df.to_csv(output_path, compression="gzip",index=False)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", type=str)
    parser.add_argument("--output-path", type=str)
    args = parser.parse_args()

    input_path = args.input_path
    output_path = args.output_path

    if not os.path.exists(input_path):
        raise ValueError(f"{input_path} does not exist.")
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    threads = []
    for file in os.listdir(input_path):
        file_name = file.split(".")[0]
        thread = threading.Thread(target=transform,
                                  args=(os.path.join(input_path, file), f"{output_path}/{file_name}.csv.gz"))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()
