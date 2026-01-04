import logging
from pathlib import Path
from pipeline.main import main
# We keep the import name, but ensure run_sql_checks.py now has Silver checks
from run_sql_checks import run_validation

if __name__ == "__main__":
    # Setup paths and log directory
    root = Path(__file__).resolve().parent
    (root / "output" / "logs").mkdir(parents=True, exist_ok=True)

    # Initialize logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(root / "output" / "logs" / "pipeline.log", encoding="utf-8"),
        ],
    )

    logger = logging.getLogger("Runner")

    try:
        logger.info("Starting Eligibility Pipeline Execution")

        # Step 1: Execute core pipeline logic (Stage 0, 1, and now Stage 2)
        # The main function now includes the call to stage2_clean_silver
        load_run_id = main(root)

        # Step 2: Automatic Data Quality Validation
        # Now verifies both Stage 1 (Bronze) and Stage 2 (Silver)
        logger.info("Running automated SQL integrity checks for Bronze and Silver layers...")
        run_validation(root, load_run_id) # Added 'root' to pass the path

        logger.info("Pipeline run completed successfully. All layers validated.")

    except Exception as e:
        logger.error("Pipeline crashed during execution", exc_info=True)
        exit(1)