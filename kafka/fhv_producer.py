import config
from common_func import *

if __name__ == '__main__':
    conf =  {
    'bootstrap.servers': config.KAFKA_BROCKERS
    }
    csv_path = '../data/raw/fhv/fhv_tripdata_2019-01.csv.gz'
    messages = get_msgs_from_csv(csv_path)

    produce_messages_json('fhv_rides_head_json',
                          messages,
                          conf,
                          delivery_report)