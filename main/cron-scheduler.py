import subprocess
from config import (
    PYTHON_PROGRAMME,
    EXTRACT_FILEPATH,
    LOAD_FILEPATH,
    VISUALISATION_FILEPATH,
)


def set_crontab(crontab_content):
    """Overwrite crontab."""

    try:
        subprocess.run(["crontab"], input=crontab_content, text=True, check=True)
        print("Crontab successfully updated!")

    except subprocess.CalledProcessError as e:
        print(f"Failed to update crontab: {e}")

    print("\nThis is the latest crontab:")
    subprocess.run("crontab -l", shell=True)


crontab_content = f"""
30 14 15 * * {PYTHON_PROGRAMME} {EXTRACT_FILEPATH}
35 14 15 * * {PYTHON_PROGRAMME} {LOAD_FILEPATH}
40 14 15 * * {PYTHON_PROGRAMME} {VISUALISATION_FILEPATH}
"""

if __name__ == "__main__":
    set_crontab(crontab_content)
