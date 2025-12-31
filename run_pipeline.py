from pathlib import Path
from pipeline.main import main

if __name__ == "__main__":
    root = Path(__file__).resolve().parent
    main(root)
