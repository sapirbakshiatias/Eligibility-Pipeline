from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
INPUT = BASE / "input"
OUTPUT = BASE / "output"
DOCS = BASE / "docs"

def main():
    OUTPUT.mkdir(exist_ok=True, parents=True)
    raise SystemExit("Implement pipeline and write outputs into output/")

if __name__ == "__main__":
    main()
