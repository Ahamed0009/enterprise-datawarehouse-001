from pathlib import Path

output_file = "project_structure.txt"

IGNORE_DIRS = {".git", "__pycache__", ".venv", ".idea", ".vscode"}

lines = []

def print_tree(directory, prefix=""):
    contents = sorted(Path(directory).iterdir(), key=lambda x: (x.is_file(), x.name.lower()))

    contents = [item for item in contents if item.name not in IGNORE_DIRS]

    for index, path in enumerate(contents):
        connector = "└── " if index == len(contents) - 1 else "├── "
        line = prefix + connector + path.name
        print(line)
        lines.append(line)

        if path.is_dir():
            extension = "    " if index == len(contents) - 1 else "│   "
            print_tree(path, prefix + extension)


root_name = Path(".").resolve().name
print(root_name)
lines.append(root_name)

print_tree(".")

with open(output_file, "w", encoding="utf-8") as f:
    for line in lines:
        f.write(line + "\n")

print(f"\nProject structure saved to: {output_file}")

