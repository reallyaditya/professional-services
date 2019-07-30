import consts
import prepare_data


def main():

    prepare_data.get_data_from_bq(
        consts.PROJECT_ID, consts.DATASET_ID, consts.INPUT_BUCKET, consts.OUTPUT_BUCKET)


main()
