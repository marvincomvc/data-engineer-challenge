from app.bronze.load import load_bronze
from app.diamond.build import build_diamond
from app.gold.build import build_gold
from app.quality.run import run_quality
from app.raw.load_raw import copy_raw_files
from app.silver.build import build_silver


def main():

    copy_raw_files()

    load_bronze()

    build_silver()

    build_gold()

    build_diamond()

    run_quality()


if __name__ == "__main__":
    main()
