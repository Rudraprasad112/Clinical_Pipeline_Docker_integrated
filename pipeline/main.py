from pipeline.ingestion import ingest as ing
from pipeline.cleaning import clean as cle
from pipeline.transformation import transform as trans
from pipeline.stats import analytics as analysis
from pipeline.stats import plots as plts

def main():
    # ingest all fils
    print("pipeline_start")
    try:
        ing.main()
        print("ingest_done")
        # clean 
        cle.main()
        print("clean_done")
        # transform applye
        trans.main()
        print("transformation_done")
        # analytics
        analysis.main()
        print("analysis_done")
        # drow plots
        plts.main()
        print("plots_done")
    except Exception as e:
        print("error:--------------",e)


if __name__ == "__main__":
    main()
