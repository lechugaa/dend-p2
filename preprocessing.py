import csv
import glob
import os
from dotenv import load_dotenv


def get_file_path_list(sub_folder):
    """
    Obtains all file paths in the provided `sub_folder`.
    :param sub_folder: String
    :return: List of path strings
    """
    # Get your current folder and sub folder event data
    filepath = os.getcwd() + '/' + sub_folder

    # Create a for loop to create a list of files and collect each filepath
    file_path_list = []
    for root, dirs, files in os.walk(filepath):
        file_path_list += glob.glob(os.path.join(root, '*'))

    return file_path_list


def read_csv_data(file_path_list):
    """
    Obtains all rows of the csv files provided in `file_path_list`.
    :param file_path_list: List[String]
    :return: List[Tuples]
    """
    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = []

    # for every filepath in the file path list
    for f in file_path_list:

        # reading csv file
        with open(f, 'r', encoding='utf8', newline='') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)
            # removing the header line
            next(csvreader)

            # extracting each data row one by one and append it
            for line in csvreader:
                full_data_rows_list.append(line)

    return full_data_rows_list


def write_full_csv_data(full_data_rows_list, full_csv_name):
    """
    Creates a smaller event data csv file called `full_csv_name` that will be used to insert data into
    the Apache Cassandra tables
    :param full_data_rows_list: List[Tuples]
    :param full_csv_name: String
    """
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open(full_csv_name, 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow([
            'artist',
            'firstName',
            'gender',
            'itemInSession',
            'lastName',
            'length',
            'level',
            'location',
            'sessionId',
            'song',
            'userId'
        ])
        for row in full_data_rows_list:
            if row[0] == '':
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


def main():
    load_dotenv()
    file_path_list = get_file_path_list(os.getenv('DATA_DIRECTORY'))
    full_data_row_list = read_csv_data(file_path_list)
    write_full_csv_data(full_data_row_list, os.getenv('FULL_CSV_NAME'))


if __name__ == '__main__':
    main()
